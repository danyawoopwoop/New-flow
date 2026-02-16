import asyncio
import json
import logging
import os
import shutil
import atexit
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from auth import auth_router
from parser_events import parser_events_router, start_parser_event_worker, stop_parser_event_worker
from threads import threads_router

BASE_DIR = Path(__file__).resolve().parent
USERS_DIR = BASE_DIR / "json bd"
RUNTIME_DIR = BASE_DIR / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
LOG_FILE = LOG_DIR / "server.log"
BACKUP_DIR = RUNTIME_DIR / "json_bd_backup"
INVALID_JSON_DIR = RUNTIME_DIR / "json_invalid"
BACKUP_INTERVAL_SEC = int(os.environ.get("JSON_BD_BACKUP_INTERVAL_SEC", "3600"))
LOG_MAX_BYTES = int(os.environ.get("APP_LOG_MAX_BYTES", str(20 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.environ.get("APP_LOG_BACKUP_COUNT", "5"))
MAIN_LOCK_FILE = RUNTIME_DIR / "main.lock"

_backup_task: asyncio.Task | None = None
_backup_stop_event: asyncio.Event | None = None
_main_lock_handle = None


def configure_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    root.addHandler(file_handler)
    root.addHandler(stream_handler)


def backup_json_bd_once() -> int:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    copied = 0
    for src in USERS_DIR.glob("*.json"):
        if not src.is_file():
            continue
        shutil.copy2(src, BACKUP_DIR / src.name)
        copied += 1
    return copied


def _read_json_dict(path: Path):
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return None, str(exc)
    if not isinstance(data, dict):
        return None, "json root is not an object"
    return data, ""


def validate_json_bd_on_startup() -> dict:
    USERS_DIR.mkdir(parents=True, exist_ok=True)
    INVALID_JSON_DIR.mkdir(parents=True, exist_ok=True)
    report = {"total": 0, "ok": 0, "restored": 0, "invalid": 0}

    for src in USERS_DIR.glob("*.json"):
        report["total"] += 1
        data, error = _read_json_dict(src)
        if data is not None:
            report["ok"] += 1
            continue

        backup = BACKUP_DIR / src.name
        backup_data, _ = _read_json_dict(backup) if backup.exists() else (None, "backup missing")
        if backup_data is not None:
            shutil.copy2(backup, src)
            report["restored"] += 1
            logger.warning("Invalid JSON restored from backup: %s", src.name)
            continue

        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        bad_dst = INVALID_JSON_DIR / f"{src.stem}.{stamp}.bad.json"
        try:
            shutil.move(str(src), str(bad_dst))
        except Exception:
            pass
        report["invalid"] += 1
        logger.error("Invalid JSON quarantined: %s (%s)", src.name, error)

    return report


async def backup_loop(stop_event: asyncio.Event) -> None:
    logger = logging.getLogger("backup")
    while not stop_event.is_set():
        try:
            copied = await asyncio.to_thread(backup_json_bd_once)
            logger.info("JSON backup updated: %s file(s)", copied)
        except Exception:
            logger.exception("JSON backup failed")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=BACKUP_INTERVAL_SEC)
        except asyncio.TimeoutError:
            continue


configure_logging()
logger = logging.getLogger("app")


def acquire_main_lock():
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    handle = open(MAIN_LOCK_FILE, "a+", encoding="utf-8")
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        handle.close()
        return None

    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def release_main_lock(handle) -> None:
    if handle is None:
        return
    try:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        handle.close()
    except Exception:
        pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _backup_task, _backup_stop_event
    report = validate_json_bd_on_startup()
    logger.info(
        "JSON startup check: total=%s ok=%s restored=%s invalid=%s",
        report["total"],
        report["ok"],
        report["restored"],
        report["invalid"],
    )
    await start_parser_event_worker()
    _backup_stop_event = asyncio.Event()
    _backup_task = asyncio.create_task(backup_loop(_backup_stop_event))
    logger.info("Application startup complete")
    try:
        yield
    finally:
        if _backup_stop_event is not None:
            _backup_stop_event.set()
        if _backup_task is not None:
            await _backup_task
            _backup_task = None
        await stop_parser_event_worker()
        logger.info("Application shutdown complete")


app = FastAPI(lifespan=lifespan)
# Change session secret for production.
app.add_middleware(SessionMiddleware, secret_key="change-this-secret-key")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

app.include_router(auth_router)
app.include_router(threads_router)
app.include_router(parser_events_router)


if __name__ == "__main__":
    import uvicorn

    _main_lock_handle = acquire_main_lock()
    if _main_lock_handle is None:
        logger.error("main.py is already running. Stop the existing process first.")
        raise SystemExit(1)

    atexit.register(release_main_lock, _main_lock_handle)

    # Listen on all interfaces for LAN access.
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False, log_config=None)
