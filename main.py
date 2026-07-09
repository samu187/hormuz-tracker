import asyncio
import json
import logging
import os
import ssl
import threading
import webbrowser
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import certifi
import uvicorn
import websockets
from dotenv import load_dotenv
from fastapi import Body, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from websockets.exceptions import ConnectionClosed


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
AIS_STREAM_URL = "wss://stream.aisstream.io/v0/stream"
HORMUZ_BOUNDING_BOX = [[[25.5, 55.0], [27.5, 57.5]]]
REGIONS: dict[str, dict[str, Any]] = {
    "hormuz": {
        "id": "hormuz",
        "name": "Strait of Hormuz",
        "title": "Strait of Hormuz Live AIS Tracker",
        "boundingBoxes": HORMUZ_BOUNDING_BOX,
        "center": [26.5, 56.3],
        "zoom": 8,
    },
    "sicily": {
        "id": "sicily",
        "name": "Sicily and Central Mediterranean",
        "title": "Sicily Live AIS Tracker",
        "boundingBoxes": [[[36.0, 11.5], [39.0, 16.5]]],
        "center": [37.5, 14.0],
        "zoom": 7,
    },
    "oman": {
        "id": "oman",
        "name": "Gulf of Oman",
        "title": "Gulf of Oman Live AIS Tracker",
        "boundingBoxes": [[[22.0, 56.0], [26.5, 61.0]]],
        "center": [24.3, 58.7],
        "zoom": 7,
    },
    "london": {
        "id": "london",
        "name": "London and Thames Estuary",
        "title": "London Thames Live AIS Tracker",
        "boundingBoxes": [[[51.2, -0.8], [51.8, 1.6]]],
        "center": [51.5, 0.25],
        "zoom": 9,
    },
    "gibraltar": {
        "id": "gibraltar",
        "name": "Gibraltar Strait",
        "title": "Gibraltar Strait Live AIS Tracker",
        "boundingBoxes": [[[35.7, -6.4], [36.4, -4.8]]],
        "center": [36.05, -5.6],
        "zoom": 9,
    },
    "iceland": {
        "id": "iceland",
        "name": "Iceland Coastal Waters",
        "title": "Iceland Live AIS Tracker",
        "boundingBoxes": [[[63.0, -25.0], [67.2, -12.0]]],
        "center": [64.9, -18.6],
        "zoom": 6,
    },
    "black_sea": {
        "id": "black_sea",
        "name": "Black Sea",
        "title": "Black Sea Live AIS Tracker",
        "boundingBoxes": [[[41.0, 27.0], [47.5, 42.5]]],
        "center": [44.2, 34.8],
        "zoom": 7,
    },
    "crimea": {
        "id": "crimea",
        "name": "Crimea and Kerch Strait",
        "title": "Crimea and Kerch Strait Live AIS Tracker",
        "boundingBoxes": [[[44.0, 32.0], [46.5, 37.5]]],
        "center": [45.2, 35.1],
        "zoom": 8,
    },
    "singapore": {
        "id": "singapore",
        "name": "Singapore Strait",
        "title": "Singapore Strait Live AIS Tracker",
        "boundingBoxes": [[[1.0, 103.4], [1.6, 104.4]]],
        "center": [1.28, 103.9],
        "zoom": 10,
    },
    "suez": {
        "id": "suez",
        "name": "Suez Canal",
        "title": "Suez Canal Live AIS Tracker",
        "boundingBoxes": [[[29.7, 32.1], [31.4, 32.7]]],
        "center": [30.55, 32.35],
        "zoom": 8,
    },
    "panama": {
        "id": "panama",
        "name": "Panama Canal",
        "title": "Panama Canal Live AIS Tracker",
        "boundingBoxes": [[[8.8, -80.2], [9.5, -79.4]]],
        "center": [9.15, -79.8],
        "zoom": 9,
    },
    "english_channel": {
        "id": "english_channel",
        "name": "English Channel",
        "title": "English Channel Live AIS Tracker",
        "boundingBoxes": [[[49.3, -5.8], [51.7, 2.0]]],
        "center": [50.5, -1.9],
        "zoom": 7,
    },
}
GLOBAL_DIAGNOSTIC_BOUNDING_BOX = [[[-90.0, -180.0], [90.0, 180.0]]]
STATUS_BROADCAST_INTERVAL_SECONDS = 5.0

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("hormuz-tracker")

load_dotenv(BASE_DIR / ".env")

stream_stats: dict[str, Any] = {
    "connected": False,
    "rawMessages": 0,
    "acceptedPositions": 0,
    "discardedMessages": 0,
    "lastRawMessageAt": None,
    "lastShipPositionAt": None,
    "lastError": None,
}
discard_reason_counts: dict[str, int] = {}
current_region_id = "hormuz"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def active_region() -> dict[str, Any]:
    return REGIONS[current_region_id]


def region_payload(region: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": region["id"],
        "name": region["name"],
        "title": region["title"],
        "boundingBoxes": region["boundingBoxes"],
        "center": region["center"],
        "zoom": region["zoom"],
    }


def active_region_payload() -> dict[str, Any]:
    return region_payload(active_region())


def available_regions_payload() -> list[dict[str, Any]]:
    return [region_payload(region) for region in REGIONS.values()]


def reset_stream_stats() -> None:
    stream_stats.update(
        {
            "connected": False,
            "rawMessages": 0,
            "acceptedPositions": 0,
            "discardedMessages": 0,
            "lastRawMessageAt": None,
            "lastShipPositionAt": None,
            "lastError": None,
            "subscription": None,
        }
    )
    discard_reason_counts.clear()


def _point_in_bounding_boxes(
    lat: float, lon: float, bounding_boxes: list[list[list[float]]]
) -> bool:
    for box in bounding_boxes:
        (lat1, lon1), (lat2, lon2) = box
        min_lat, max_lat = sorted((lat1, lat2))
        min_lon, max_lon = sorted((lon1, lon2))
        if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
            return True
    return False


def stream_status_payload() -> dict[str, Any]:
    return {
        "type": "ais_status",
        "connected": stream_stats["connected"],
        "region": active_region_payload(),
        "subscription": stream_stats.get("subscription"),
        "rawMessages": stream_stats["rawMessages"],
        "acceptedPositions": stream_stats["acceptedPositions"],
        "discardedMessages": stream_stats["discardedMessages"],
        "discardReasons": discard_reason_counts,
        "lastRawMessageAt": stream_stats["lastRawMessageAt"],
        "lastShipPositionAt": stream_stats["lastShipPositionAt"],
        "lastError": stream_stats["lastError"],
    }


class ConnectionManager:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._clients.add(websocket)
        await websocket.send_text(json.dumps(stream_status_payload(), separators=(",", ":")))
        logger.info("Frontend connected; clients=%s", await self.count())

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(websocket)
        logger.info("Frontend disconnected; clients=%s", await self.count())

    async def count(self) -> int:
        async with self._lock:
            return len(self._clients)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        message = json.dumps(payload, separators=(",", ":"))
        async with self._lock:
            clients = list(self._clients)

        if not clients:
            return

        stale_clients: list[WebSocket] = []
        send_tasks = [self._send(client, message, stale_clients) for client in clients]
        await asyncio.gather(*send_tasks, return_exceptions=True)

        if stale_clients:
            async with self._lock:
                for client in stale_clients:
                    self._clients.discard(client)

    async def _send(
        self, websocket: WebSocket, message: str, stale_clients: list[WebSocket]
    ) -> None:
        try:
            await websocket.send_text(message)
        except Exception as exc:
            logger.warning("Dropping stale frontend websocket: %s", exc)
            stale_clients.append(websocket)


manager = ConnectionManager()


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None and value != "":
            return value
    return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _normalise_heading(true_heading: Any, cog: Any) -> float:
    heading = _as_float(true_heading)
    if heading is not None and 0 <= heading < 360:
        return heading

    course = _as_float(cog)
    if course is not None and 0 <= course < 360:
        return course

    return 0.0


def _discard(reason: str) -> None:
    discard_reason_counts[reason] = discard_reason_counts.get(reason, 0) + 1
    if discard_reason_counts[reason] <= 5:
        logger.info("Discarding AIS message: %s", reason)


def _env_true(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _subscription_payload(api_key: str) -> dict[str, Any]:
    region = active_region()
    bounding_boxes = region["boundingBoxes"]
    if _env_true("AIS_DIAGNOSTIC_GLOBAL_BBOX"):
        bounding_boxes = GLOBAL_DIAGNOSTIC_BOUNDING_BOX
        logger.warning(
            "AIS_DIAGNOSTIC_GLOBAL_BBOX is enabled; requesting global AIS traffic. "
            "Disable it for Hormuz-only production tracking."
        )

    payload: dict[str, Any] = {
        "APIKey": api_key,
        "BoundingBoxes": bounding_boxes,
    }

    if not _env_true("AIS_DIAGNOSTIC_ALL_MESSAGE_TYPES"):
        payload["FilterMessageTypes"] = ["PositionReport"]
    else:
        logger.warning(
            "AIS_DIAGNOSTIC_ALL_MESSAGE_TYPES is enabled; requesting all AIS message types. "
            "Disable it for PositionReport-only production tracking."
        )

    stream_stats["subscription"] = {
        "region": active_region_payload(),
        "boundingBoxes": bounding_boxes,
        "filterMessageTypes": payload.get("FilterMessageTypes", "ALL"),
        "diagnosticGlobalBoundingBox": _env_true("AIS_DIAGNOSTIC_GLOBAL_BBOX"),
        "diagnosticAllMessageTypes": _env_true("AIS_DIAGNOSTIC_ALL_MESSAGE_TYPES"),
    }
    return payload


def _clean_position_report(raw: dict[str, Any]) -> dict[str, Any] | None:
    if raw.get("MessageType") != "PositionReport":
        _discard(f"message type is {raw.get('MessageType')!r}")
        return None

    metadata = raw.get("MetaData") or {}
    message = raw.get("Message") or {}
    report = message.get("PositionReport") or raw.get("PositionReport") or {}

    mmsi = _as_int(_first_present(report.get("UserID"), metadata.get("MMSI"), raw.get("MMSI")))
    lat = _as_float(
        _first_present(
            report.get("Latitude"),
            report.get("latitude"),
            metadata.get("Latitude"),
            metadata.get("latitude"),
        )
    )
    lon = _as_float(
        _first_present(
            report.get("Longitude"),
            report.get("longitude"),
            metadata.get("Longitude"),
            metadata.get("longitude"),
        )
    )

    if mmsi is None or lat is None or lon is None:
        _discard(
            "missing required position fields "
            f"(mmsi={mmsi!r}, lat={lat!r}, lon={lon!r})"
        )
        return None

    region = active_region()
    if not _point_in_bounding_boxes(lat, lon, region["boundingBoxes"]):
        _discard(f"outside {region['name']} bounds (lat={lat}, lon={lon})")
        return None

    cog = _as_float(report.get("Cog"))
    sog = _as_float(report.get("Sog"))
    true_heading = _as_float(report.get("TrueHeading"))
    ship_name = str(
        _first_present(metadata.get("ShipName"), metadata.get("ship_name"), "Unknown")
    ).strip()
    timestamp = _first_present(
        metadata.get("time_utc"),
        metadata.get("Time_UTC"),
        metadata.get("timestamp"),
        raw.get("time_utc"),
        datetime.now(timezone.utc).isoformat(),
    )

    return {
        "type": "ship_position",
        "mmsi": mmsi,
        "name": ship_name or "Unknown",
        "lat": lat,
        "lon": lon,
        "sog": sog,
        "cog": cog,
        "trueHeading": true_heading,
        "heading": _normalise_heading(true_heading, cog),
        "timestamp": timestamp,
    }


async def aisstream_client(stop_event: asyncio.Event) -> None:
    api_key = os.getenv("AIS_API_KEY")
    if not api_key or api_key == "replace_with_your_aisstream_api_key":
        error_message = "AIS_API_KEY is not configured. Set it in .env before starting live tracking."
        stream_stats["lastError"] = error_message
        logger.error(error_message)
        await stop_event.wait()
        return

    subscription = _subscription_payload(api_key)
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    reconnect_delay = 1.0
    next_status_broadcast = 0.0
    while not stop_event.is_set():
        try:
            logger.info(
                "Connecting to AIS stream with subscription: %s",
                stream_stats["subscription"],
            )
            async with websockets.connect(
                AIS_STREAM_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=2_000_000,
                ssl=ssl_context,
            ) as upstream:
                await upstream.send(json.dumps(subscription))
                stream_stats["connected"] = True
                stream_stats["lastError"] = None
                logger.info("Subscribed to aisstream.io PositionReport feed")
                await manager.broadcast(stream_status_payload())
                reconnect_delay = 1.0

                while not stop_event.is_set():
                    try:
                        raw_message = await asyncio.wait_for(upstream.recv(), timeout=45)
                    except asyncio.TimeoutError:
                        pong_waiter = await upstream.ping()
                        await asyncio.wait_for(pong_waiter, timeout=10)
                        await manager.broadcast(stream_status_payload())
                        continue

                    try:
                        decoded = json.loads(raw_message)
                    except json.JSONDecodeError:
                        stream_stats["discardedMessages"] += 1
                        logger.warning("Discarding non-JSON AIS message")
                        continue

                    stream_stats["rawMessages"] += 1
                    stream_stats["lastRawMessageAt"] = _utc_now()
                    ship = _clean_position_report(decoded)
                    if ship is not None:
                        stream_stats["acceptedPositions"] += 1
                        stream_stats["lastShipPositionAt"] = _utc_now()
                        await manager.broadcast(ship)
                    else:
                        stream_stats["discardedMessages"] += 1

                    loop_time = asyncio.get_running_loop().time()
                    if loop_time >= next_status_broadcast:
                        logger.info(
                            "AIS stream stats: raw=%s accepted=%s discarded=%s clients=%s",
                            stream_stats["rawMessages"],
                            stream_stats["acceptedPositions"],
                            stream_stats["discardedMessages"],
                            await manager.count(),
                        )
                        await manager.broadcast(stream_status_payload())
                        next_status_broadcast = loop_time + STATUS_BROADCAST_INTERVAL_SECONDS

        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, OSError, TimeoutError, websockets.InvalidHandshake) as exc:
            stream_stats["connected"] = False
            stream_stats["lastError"] = str(exc)
            logger.warning("AIS upstream disconnected: %s", exc)
        except Exception:
            stream_stats["connected"] = False
            stream_stats["lastError"] = "Unexpected AIS upstream failure"
            logger.exception("Unexpected AIS upstream failure")

        if stop_event.is_set():
            break

        logger.info("Reconnecting AIS upstream in %.1f seconds", reconnect_delay)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, 60.0)


async def restart_aisstream(app: FastAPI) -> None:
    old_stop_event: asyncio.Event | None = getattr(app.state, "stop_event", None)
    old_upstream_task: asyncio.Task | None = getattr(app.state, "upstream_task", None)

    if old_stop_event is not None:
        old_stop_event.set()
    if old_upstream_task is not None:
        old_upstream_task.cancel()
        with suppress(asyncio.CancelledError):
            await old_upstream_task

    reset_stream_stats()
    stop_event = asyncio.Event()
    upstream_task = asyncio.create_task(aisstream_client(stop_event), name="aisstream-client")
    app.state.stop_event = stop_event
    app.state.upstream_task = upstream_task
    await manager.broadcast(stream_status_payload())


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_event = asyncio.Event()
    upstream_task = asyncio.create_task(aisstream_client(stop_event), name="aisstream-client")
    app.state.stop_event = stop_event
    app.state.upstream_task = upstream_task

    try:
        yield
    finally:
        logger.info("Shutting down Hormuz tracker")
        current_stop_event: asyncio.Event = getattr(app.state, "stop_event", stop_event)
        current_upstream_task: asyncio.Task = getattr(app.state, "upstream_task", upstream_task)
        current_stop_event.set()
        current_upstream_task.cancel()
        with suppress(asyncio.CancelledError):
            await current_upstream_task


app = FastAPI(title="Strait of Hormuz Ship Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "region": active_region_payload(),
            "boundingBox": active_region()["boundingBoxes"],
            "frontendClients": await manager.count(),
            "aisApiKeyConfigured": bool(os.getenv("AIS_API_KEY"))
            and os.getenv("AIS_API_KEY") != "replace_with_your_aisstream_api_key",
            "stream": stream_status_payload(),
        }
    )


@app.get("/config")
async def config() -> JSONResponse:
    return JSONResponse(active_region_payload())


@app.get("/regions")
async def regions() -> JSONResponse:
    return JSONResponse(
        {
            "activeRegion": active_region_payload(),
            "regions": available_regions_payload(),
        }
    )


@app.post("/region")
async def set_region(
    request: Request,
    payload: dict[str, Any] = Body(...),
) -> JSONResponse:
    global current_region_id

    region_id = str(payload.get("region") or payload.get("id") or "").strip().lower()
    if region_id not in REGIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown region {region_id!r}. Use one of: {', '.join(REGIONS)}",
        )

    if region_id != current_region_id:
        logger.info("Switching active AIS region from %s to %s", current_region_id, region_id)
        current_region_id = region_id
        await restart_aisstream(request.app)

    return JSONResponse(active_region_payload())


@app.websocket("/ws/ships")
async def ships_websocket(websocket: WebSocket) -> None:
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
    except Exception as exc:
        logger.warning("Frontend websocket closed unexpectedly: %s", exc)
        await manager.disconnect(websocket)


def run() -> None:
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    browser_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    url = f"http://{browser_host}:{port}"

    if os.getenv("OPEN_BROWSER", "true").strip().lower() not in {"0", "false", "no", "off"}:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    uvicorn.run(
        "main:app",
        host=host,
        port=port,
    )


def run_dev() -> None:
    uvicorn.run(
        "main:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        reload=True,
        reload_dirs=[str(BASE_DIR)],
        reload_excludes=[
            ".venv",
            ".venv/*",
            ".venv/**",
            "__pycache__",
            "__pycache__/*",
            "*.pyc",
        ],
    )
