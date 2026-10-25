import asyncio
import json
import os
import time

from pymodbus.client import AsyncModbusTcpClient

# Функція Modbus, яку викликає кожна "область"
READERS = {
    "holding": "read_holding_registers",
    "input": "read_input_registers",
    "coil": "read_coils",
    "discrete": "read_discrete_inputs",
}

# DataType -> (кількість регістрів, тип у pymodbus)
REGISTER_TYPES = {
    "Short":  (1, "INT16"),
    "UShort": (1, "UINT16"),
    "Int":    (2, "INT32"),
    "UInt":   (2, "UINT32"),
    "Float":  (2, "FLOAT32"),
    "Double": (4, "FLOAT64"),
}

REQUIRED_FIELDS = ("Tag", "Unit", "Area", "Address", "DataType")
MIN_INTERVAL_MS = 50

def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------- конфігурація

def validate_sensors(data) -> list:
    """Приймає {"Sensors": [...]} або просто масив. Повертає список датчиків або кидає ValueError."""
    sensors = data.get("Sensors") if isinstance(data, dict) else data

    if not isinstance(sensors, list):
        raise ValueError('очікується {"Sensors": [...]} або масив датчиків')

    seen = set()
    for i, s in enumerate(sensors):
        if not isinstance(s, dict):
            raise ValueError(f"датчик #{i}: очікується об'єкт")

        for field in REQUIRED_FIELDS:
            if field not in s:
                raise ValueError(f"датчик #{i}: немає поля {field}")

        tag = s["Tag"]
        if not isinstance(tag, str) or not tag or "/" in tag:
            raise ValueError(f"датчик #{i}: Tag має бути непорожнім рядком без '/'")
        if tag in seen:
            raise ValueError(f"датчик '{tag}': Tag повторюється")
        seen.add(tag)

        if s["Area"] not in READERS:
            raise ValueError(f"датчик '{tag}': Area має бути одним із {list(READERS)}")

        is_bit = s["Area"] in ("coil", "discrete")
        if is_bit and s["DataType"] != "Bool":
            raise ValueError(f"датчик '{tag}': для coil/discrete DataType має бути Bool")
        if not is_bit and s["DataType"] not in REGISTER_TYPES:
            raise ValueError(f"датчик '{tag}': DataType має бути одним із {list(REGISTER_TYPES)}")

        for field in ("Unit", "Address"):
            if not isinstance(s[field], int) or isinstance(s[field], bool) or s[field] < 0:
                raise ValueError(f"датчик '{tag}': {field} має бути цілим числом >= 0")

        if s.get("WordOrder", "big") not in ("big", "little"):
            raise ValueError(f"датчик '{tag}': WordOrder має бути big або little")

    return sensors


def validate_interval(data):
    """Інтервал опитування в мс із {"UpdateRateMs": ...} або None, якщо поля немає."""
    if not isinstance(data, dict) or "UpdateRateMs" not in data:
        return None

    value = data["UpdateRateMs"]
    if not isinstance(value, int) or isinstance(value, bool) or value < MIN_INTERVAL_MS:
        raise ValueError(f"UpdateRateMs має бути цілим числом >= {MIN_INTERVAL_MS}")
    return value


def load_sensors(path: str) -> list:
    with open(path, encoding="utf-8") as f:
        return validate_sensors(json.load(f))


def load_interval_ms(path: str):
    with open(path, encoding="utf-8") as f:
        return validate_interval(json.load(f))


def save_sensors(path: str, sensors: list, interval_ms: int) -> None:
    # Атомарний запис: спершу тимчасовий файл, потім заміна, щоб не лишити половину файлу
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"Sensors": sensors, "UpdateRateMs": interval_ms}, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ---------------------------------------------------------------- читання Modbus

async def read_sensor(client, s):
    """Читає один датчик і повертає число. Кидає виняток при помилці."""
    read = getattr(client, READERS[s["Area"]])

    # Біти (coil, discrete input)
    if s["Area"] in ("coil", "discrete"):
        rr = await read(s["Address"], count=1, device_id=s["Unit"])
        if rr.isError():
            raise IOError(str(rr))
        return 1.0 if rr.bits[0] else 0.0

    # Регістри: кількість і спосіб розбору залежать від типу
    count, type_name = REGISTER_TYPES[s["DataType"]]
    rr = await read(s["Address"], count=count, device_id=s["Unit"])
    if rr.isError():
        raise IOError(str(rr))

    kind = getattr(client.DATATYPE, type_name)
    value = client.convert_from_registers(
        rr.registers, kind, word_order=s.get("WordOrder", "big")
    )
    return round(value, 2)


async def write_value(client, s, value):
    """Записує значення в датчик. Кидає виняток при помилці."""
    if s["Area"] == "coil":
        rr = await client.write_coil(s["Address"], bool(value), device_id=s["Unit"])
    elif s["Area"] == "holding":
        count, type_name = REGISTER_TYPES[s["DataType"]]
        kind = getattr(client.DATATYPE, type_name)
        if type_name.startswith(("INT", "UINT")):
            value = int(value)   # цілі типи: дробова частина відкидається
        registers = client.convert_to_registers(value, kind, word_order=s.get("WordOrder", "big"))
        if len(registers) == 1:
            rr = await client.write_register(s["Address"], registers[0], device_id=s["Unit"])
        else:
            rr = await client.write_registers(s["Address"], registers, device_id=s["Unit"])
    else:
        raise ValueError("запис можливий лише в coil і holding")

    if rr.isError():
        raise IOError(str(rr))

class Poller:
    """Циклічно опитує Modbus і повідомляє про результати через колбеки.

    on_batch(message)  – після кожного циклу: {"type": "update", ...}
    on_status(message) – коли змінився стан зв'язку з Modbus: {"type": "status", ...}
    """

    def __init__(self, host, port, sensors, interval=0.5):
        self.host = host
        self.port = port
        self.sensors = sensors
        self.interval = interval
        self.connected = False
        self.client = None
        self.lock = asyncio.Lock()   # опитування і запис не перетинаються на одному з'єднанні
        self.values = {}      # tag -> {"tag", "value", "quality", "ts"}
        self.on_batch = None
        self.on_status = None

    def set_sensors(self, sensors: list) -> None:
        """Підміняє перелік датчиків без перезапуску. Дані видалених датчиків прибираються."""
        self.sensors = sensors
        tags = {s["Tag"] for s in sensors}
        for tag in list(self.values):
            if tag not in tags:
                del self.values[tag]
    async def write_sensor(self, tag: str, value: float) -> None:
        """KeyError: немає такого датчика; ConnectionError: немає зв'язку з Modbus."""
        sensor = next((s for s in self.sensors if s["Tag"] == tag), None)
        if sensor is None:
            raise KeyError(tag)
        if self.client is None or not self.client.connected:
            raise ConnectionError("немає зв'язку з Modbus-пристроєм")
    
        async with self.lock:
            await write_value(self.client, sensor, value)

    def snapshot(self) -> list:
        return [dict(entry) for entry in self.values.values()]   # копії, щоб не змінились під час відправки

    async def _set_connected(self, connected: bool) -> None:
        if connected == self.connected:
            return
        self.connected = connected
        if self.on_status:
            await self.on_status({"type": "status", "modbus": "connected" if connected else "disconnected"})

        # Без зв'язку з Modbus усі значення застарілі: лишаємо останнє значення, але з quality = bad
        if not connected and self.values:
            ts = now_ms()
            for entry in self.values.values():
                entry["quality"] = "bad"
                entry["ts"] = ts
            if self.on_batch:
                await self.on_batch({"type": "update", "ts": ts, "values": self.snapshot()})

    async def _poll_once(self, client) -> None:
        ts = now_ms()
        batch = []

        for s in list(self.sensors):   # копія: перелік можуть підмінити посеред циклу
            try:
                async with self.lock:
                        value, quality = await read_sensor(client, s), "good"
            except Exception as ex:
                # Помилка одного датчика не зупиняє решту
                print(f"{s['Tag']}: ПОМИЛКА {ex}")
                value, quality = None, "bad"

            entry = {"tag": s["Tag"], "value": value, "quality": quality, "ts": ts}
            self.values[s["Tag"]] = entry
            batch.append(entry)

        if self.on_batch:
            await self.on_batch({"type": "update", "ts": ts, "values": batch})

    async def run(self) -> None:
        client = AsyncModbusTcpClient(self.host, port=self.port)
        self.client = client

        try:
            while True:
                if not client.connected:
                    await client.connect()
                    if not client.connected:
                        print(f"Немає зв'язку з {self.host}:{self.port}, повтор через 3 с")
                        await self._set_connected(False)
                        await asyncio.sleep(3)
                        continue

                await self._set_connected(True)

                started = time.monotonic()
                await self._poll_once(client)

                # Витримуємо інтервал незалежно від того, скільки тривало опитування
                await asyncio.sleep(max(0.0, self.interval - (time.monotonic() - started)))
        finally:
            client.close()
