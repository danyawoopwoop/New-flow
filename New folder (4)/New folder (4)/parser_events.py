import asyncio
import os
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from accsparser import _load_users as _load_accounts_users
from accsparser import _update_users_accounts_stats
from ParserPost import _load_users as _load_posts_users
from ParserPost import _update_users_posts_stats

parser_events_router = APIRouter()

_QUEUE_SIZE = int(os.environ.get("PARSER_EVENT_QUEUE_SIZE", "200"))
_EVENT_QUEUE: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_QUEUE_SIZE)
_WORKER_TASK: asyncio.Task | None = None
_STOP_EVENT = asyncio.Event()
_LAST_OK_UTC = ""
_LAST_ERROR = ""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_stats_map(raw: Any) -> dict:
    if not isinstance(raw, dict):
        return {}
    out = {}
    for key, value in raw.items():
        target = str(key).strip()
        if not target:
            continue
        if isinstance(value, dict):
            out[target] = value
    return out


def _apply_event_sync(event: dict) -> None:
    event_type = str(event.get("type", "")).strip()
    stats_by_url = _normalize_stats_map(event.get("stats_by_url"))
    if not stats_by_url:
        return

    if event_type == "accounts_stats_batch":
        users = _load_accounts_users()
        _update_users_accounts_stats(users, stats_by_url)
        return

    if event_type == "posts_stats_batch":
        users = _load_posts_users()
        _update_users_posts_stats(users, stats_by_url)
        return

    raise ValueError(f"unsupported event type: {event_type}")


async def _worker_loop() -> None:
    global _LAST_OK_UTC, _LAST_ERROR
    while not _STOP_EVENT.is_set():
        try:
            event = await asyncio.wait_for(_EVENT_QUEUE.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue

        try:
            await asyncio.to_thread(_apply_event_sync, event)
            _LAST_OK_UTC = _utc_now_iso()
            _LAST_ERROR = ""
        except Exception as exc:
            _LAST_ERROR = f"{type(exc).__name__}: {exc}"
            print(f"[parser-events] apply failed: {_LAST_ERROR}")
        finally:
            _EVENT_QUEUE.task_done()


async def start_parser_event_worker() -> None:
    global _WORKER_TASK, _STOP_EVENT
    if _WORKER_TASK is not None and not _WORKER_TASK.done():
        return
    _STOP_EVENT = asyncio.Event()
    _WORKER_TASK = asyncio.create_task(_worker_loop())


async def stop_parser_event_worker() -> None:
    global _WORKER_TASK
    if _WORKER_TASK is None:
        return
    _STOP_EVENT.set()
    await _WORKER_TASK
    _WORKER_TASK = None


async def enqueue_parser_event(event: dict) -> tuple[bool, str]:
    if not isinstance(event, dict):
        return False, "invalid_payload"
    event_type = str(event.get("type", "")).strip()
    if event_type not in {"accounts_stats_batch", "posts_stats_batch"}:
        return False, "invalid_type"
    if not _normalize_stats_map(event.get("stats_by_url")):
        return False, "empty_stats"
    try:
        _EVENT_QUEUE.put_nowait(event)
        return True, "queued"
    except asyncio.QueueFull:
        return False, "queue_full"


@parser_events_router.post("/internal/parser-events", name="parser_events.ingest")
async def parser_events_ingest(request: Request):
    expected_token = os.environ.get("PARSER_INTERNAL_TOKEN", "").strip()
    incoming_token = request.headers.get("X-Parser-Token", "").strip()
    if expected_token and incoming_token != expected_token:
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_json"}, status_code=400)

    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "invalid_payload"}, status_code=400)

    payload.setdefault("received_at_utc", _utc_now_iso())
    ok, reason = await enqueue_parser_event(payload)
    if not ok:
        status_code = 429 if reason == "queue_full" else 400
        return JSONResponse({"ok": False, "error": reason}, status_code=status_code)

    return {"ok": True, "status": "queued", "queue_size": _EVENT_QUEUE.qsize()}


@parser_events_router.get("/internal/parser-events/status", name="parser_events.status")
async def parser_events_status():
    return {
        "ok": True,
        "queue_size": _EVENT_QUEUE.qsize(),
        "last_ok_utc": _LAST_OK_UTC,
        "last_error": _LAST_ERROR,
        "worker_running": _WORKER_TASK is not None and not _WORKER_TASK.done(),
    }
