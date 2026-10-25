"""Мережевий шлюз: опитує Modbus і віддає дані застосунку.

  WebSocket  /ws       потік значень (Pi -> застосунок)
  GET        /health   стан шлюзу
  GET        /config   поточний перелік датчиків
  PUT        /config   замінити перелік датчиків (застосунок -> Pi)

Запуск:  python gateway.py --host 127.0.0.1 --port 10502
"""
import argparse
import asyncio
import contextlib
import os
import uuid
from poller import Poller, load_interval_ms, load_sensors, now_ms, save_sensors, validate_interval, validate_sensors

from aiohttp import WSMsgType, web

from poller import Poller, load_sensors, now_ms, save_sensors, validate_sensors

PROTOCOL_VERSION = 1

POLLER = web.AppKey("poller", Poller)
CLIENTS = web.AppKey("clients", set)
DEVICE_ID = web.AppKey("device_id", str)
SENSORS_PATH = web.AppKey("sensors_path", str)


# ---------------------------------------------------------------- ідентифікатор пристрою

def load_device_id(path: str) -> str:
    """Постійний ID цієї Pi. Створюється один раз і не залежить від Tailscale."""
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            value = f.read().strip()
            if value:
                return value

    value = str(uuid.uuid4())
    with open(path, "w", encoding="utf-8") as f:
        f.write(value)
    return value


# ---------------------------------------------------------------- розсилка

async def broadcast(app: web.Application, message: dict) -> None:
    clients = list(app[CLIENTS])
    if not clients:
        return

    # Недоступний клієнт не повинен ламати розсилку іншим
    results = await asyncio.gather(*(ws.send_json(message) for ws in clients), return_exceptions=True)
    for ws, result in zip(clients, results):
        if isinstance(result, Exception):
            app[CLIENTS].discard(ws)


# ---------------------------------------------------------------- обробники

async def health(request: web.Request) -> web.Response:
    poller = request.app[POLLER]
    return web.json_response({
        "deviceId": request.app[DEVICE_ID],
        "version": PROTOCOL_VERSION,
        "modbus": "connected" if poller.connected else "disconnected",
        "sensors": len(poller.sensors),
        "clients": len(request.app[CLIENTS]),
    })


async def get_config(request: web.Request) -> web.Response:
    poller = request.app[POLLER]
    return web.json_response({"Sensors": poller.sensors, "UpdateRateMs": interval_ms(poller)})


async def put_config(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        sensors = validate_sensors(data)
        new_interval = validate_interval(data)   # None, якщо застосунок його не надіслав
    except ValueError as ex:   # сюди ж потрапляє некоректний JSON
        return web.json_response({"error": str(ex)}, status=400)

    poller = request.app[POLLER]
    poller.set_sensors(sensors)
    if new_interval is not None:
        poller.interval = new_interval / 1000   # діє з наступного циклу

    save_sensors(request.app[SENSORS_PATH], sensors, interval_ms(poller))
    return web.json_response({"ok": True, "sensors": len(sensors), "UpdateRateMs": interval_ms(poller)})
    
async def put_value(request: web.Request) -> web.Response:
    tag = request.match_info["tag"]

    try:
        value = (await request.json())["value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError
    except (ValueError, KeyError, TypeError):
        return web.json_response({"error": 'очікується {"value": число}'}, status=400)

    try:
        await request.app[POLLER].write_sensor(tag, value)
    except KeyError:
        return web.json_response({"error": f"немає датчика '{tag}'"}, status=404)
    except ConnectionError as ex:
        return web.json_response({"error": str(ex)}, status=503)
    except ValueError as ex:
        return web.json_response({"error": str(ex)}, status=400)
    except Exception as ex:   # помилка Modbus або перетворення значення
        print(f"{tag}: ПОМИЛКА запису {ex}")
        return web.json_response({"error": str(ex)}, status=502)

    return web.json_response({"ok": True})

def interval_ms(poller: Poller) -> int:
    return int(round(poller.interval * 1000))
    
async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    app = request.app
    poller = app[POLLER]

    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)

    # Одразу даємо поточний стан, щоб застосунок не чекав наступного циклу опитування
    await ws.send_json({
        "type": "hello",
        "version": PROTOCOL_VERSION,
        "deviceId": app[DEVICE_ID],
        "modbus": "connected" if poller.connected else "disconnected",
    })
    await ws.send_json({"type": "snapshot", "ts": now_ms(), "values": poller.snapshot()})

    app[CLIENTS].add(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                pass   # тут пізніше з'являться команди запису від застосунку
    finally:
        app[CLIENTS].discard(ws)

    return ws


# ---------------------------------------------------------------- запуск

async def poller_lifecycle(app: web.Application):
    """Опитування працює у фоні, поки працює сервер."""
    task = asyncio.create_task(app[POLLER].run())
    yield
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def make_app(args) -> web.Application:
    app = web.Application()
    app[DEVICE_ID] = load_device_id(args.device_id_file)
    app[SENSORS_PATH] = args.sensors
    app[CLIENTS] = set()

    # Інтервал зі збереженого файлу важливіший за параметр --interval
    saved_ms = load_interval_ms(args.sensors)
    interval = saved_ms / 1000 if saved_ms is not None else args.interval
    
    poller = Poller(args.host, args.port, load_sensors(args.sensors), interval)
    poller.on_batch = lambda message: broadcast(app, message)
    poller.on_status = lambda message: broadcast(app, message)
    app[POLLER] = poller

    app.add_routes([
        web.get("/health", health),
        web.get("/config", get_config),
        web.put("/config", put_config),
        web.get("/ws", ws_handler),
        web.put("/sensors/{tag}/value", put_value),
    ])
    app.cleanup_ctx.append(poller_lifecycle)
    return app


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1", help="адреса Modbus-пристрою")
    p.add_argument("--port", type=int, default=10502, help="порт Modbus-пристрою")
    p.add_argument("--sensors", default="sensors.json")
    p.add_argument("--interval", type=float, default=0.5, help="секунд між циклами опитування")
    p.add_argument("--listen", default="0.0.0.0", help="на якій адресі слухає шлюз")
    p.add_argument("--listen-port", type=int, default=8765)
    p.add_argument("--device-id-file", default="device_id.txt")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    web.run_app(make_app(args), host=args.listen, port=args.listen_port)
