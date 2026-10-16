import argparse
import asyncio
import json
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

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=10502)
    p.add_argument("--sensors", default="sensors.json")
    p.add_argument("--interval", type=float, default=0.5, help="секунд між циклами опитування")
    return p.parse_args()


def load_sensors(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Приймаємо і {"Sensors": [...]}, і просто масив
    sensors = data["Sensors"] if isinstance(data, dict) else data

    if not sensors or not all(isinstance(s, dict) for s in sensors):
        raise SystemExit(
            "sensors.json: очікується масив об'єктів з полями Tag, Unit, Area, Address, DataType"
        )
    return sensors


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

async def main():
    args = parse_args()
    sensors = load_sensors(args.sensors)

    client = AsyncModbusTcpClient(args.host, port=args.port)

    while True:
        if not client.connected:
            await client.connect()
            if not client.connected:
                print(f"Немає зв'язку з {args.host}:{args.port}, повтор через 3 с")
                await asyncio.sleep(3)
                continue

        started = time.monotonic()
        for s in sensors:
            try:
                value = await read_sensor(client, s)
                print(f"{s['Tag']} = {value}")
            except Exception as ex:
                # Помилка одного датчика не зупиняє решту
                print(f"{s['Tag']}: ПОМИЛКА {ex}")

        # Витримуємо інтервал незалежно від того, скільки тривало опитування
        await asyncio.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    asyncio.run(main())
