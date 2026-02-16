import subprocess
import sys
import platform
import os
import json
import threading
import time
import shlex
import shutil
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
POSTS_PARSER_PATH = BASE_DIR / "ParserPost.py"
ACCOUNTS_PARSER_PATH = BASE_DIR / "ACCSPARSER.PY"
RUNTIME_DIR = BASE_DIR / "runtime"
POSTS_HEALTH_PATH = RUNTIME_DIR / "parser_health_posts.json"
ACCOUNTS_HEALTH_PATH = RUNTIME_DIR / "parser_health_accounts.json"
USERS_DIR = BASE_DIR / "json bd"
LOG_DIR = RUNTIME_DIR / "logs"
LOG_FILE = LOG_DIR / "server.log"
LOG_MAX_BYTES = int(os.environ.get("APP_LOG_MAX_BYTES", str(20 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.environ.get("APP_LOG_BACKUP_COUNT", "5"))

_posts_process = None
_accounts_process = None
_posts_started_at = None
_accounts_started_at = None
_posts_log_handle = None
_accounts_log_handle = None
_watchdog_started = False
_posts_expected_running = False
_accounts_expected_running = False

RESTART_INTERVAL_SEC = 60 * 60
RESTART_PAUSE_SEC = 5
HEALTH_TIMEOUT_SEC = 5 * 60
HEALTH_START_GRACE_SEC = 90


def _open_log_handle():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    # Rotate if needed before opening append stream for child process logs.
    try:
        if LOG_FILE.exists() and LOG_FILE.stat().st_size >= LOG_MAX_BYTES:
            rotator = RotatingFileHandler(
                LOG_FILE,
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding="utf-8",
            )
            rotator.doRollover()
            rotator.close()
    except Exception:
        pass
    return open(LOG_FILE, "a", encoding="utf-8", buffering=1)


def _close_log_handle(handle):
    if handle is None:
        return
    try:
        handle.close()
    except Exception:
        pass


def _parser_python_cmd() -> list:
    override = os.environ.get("PARSER_PYTHON", "").strip()
    if override:
        try:
            return shlex.split(override)
        except Exception:
            return [override]
    if platform.system().lower().startswith("win") and shutil.which("py"):
        return ["py", "-3.13"]
    return [sys.executable]


def _is_running(proc) -> bool:
    return proc is not None and proc.poll() is None


def _system_has_process(script_path: Path) -> bool:
    return bool(_find_pids_for_script(script_path))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_to_epoch(value: str):
    try:
        if not value:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def _read_health(path: Path) -> dict:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _has_posts_tasks() -> bool:
    if not USERS_DIR.exists():
        return False
    for path in USERS_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        posts = data.get("posts", []) if isinstance(data, dict) else []
        if isinstance(posts, list) and any(str(item).strip() for item in posts):
            return True
    return False


def _has_accounts_tasks() -> bool:
    if not USERS_DIR.exists():
        return False
    for path in USERS_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        accounts = data.get("accounts", []) if isinstance(data, dict) else []
        if isinstance(accounts, list) and any(str(item).strip() for item in accounts):
            return True
    return False


def _health_stale(path: Path, started_at: float | None):
    now = time.time()
    if started_at and now - started_at < HEALTH_START_GRACE_SEC:
        return False, ""

    health = _read_health(path)
    if not health:
        return True, "health_missing"

    req_epoch = _parse_iso_to_epoch(str(health.get("last_request_ts_utc", "")))
    resp_epoch = _parse_iso_to_epoch(str(health.get("last_response_ts_utc", "")))

    if req_epoch is None:
        return True, "no_request"
    if now - req_epoch > HEALTH_TIMEOUT_SEC:
        return True, "request_timeout"

    if resp_epoch is None:
        return True, "no_response"
    if now - resp_epoch > HEALTH_TIMEOUT_SEC:
        return True, "response_timeout"

    return False, ""


def _find_pids_for_script(script_path: Path):
    fragment = script_path.name.lower()
    pids = []
    try:
        if platform.system().lower().startswith("win"):
            out = ""
            try:
                out = subprocess.check_output(
                    ["wmic", "process", "get", "ProcessId,CommandLine"],
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                ).decode(errors="ignore")
            except Exception:
                out = subprocess.check_output(
                    [
                        "powershell",
                        "-NoProfile",
                        "-Command",
                        "Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine | Format-Table -HideTableHeaders",
                    ],
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                ).decode(errors="ignore")

            for line in out.splitlines():
                line_low = line.lower()
                if not line_low or fragment not in line_low:
                    continue
                parts = line.strip().split()
                if not parts:
                    continue
                # В выводе PID обычно последним полем.
                for token in reversed(parts):
                    try:
                        pid = int(token)
                        pids.append(pid)
                        break
                    except Exception:
                        continue
        else:
            out = subprocess.check_output(
                ["ps", "ax", "-o", "pid,command"], text=True, errors="ignore"
            )
            for line in out.splitlines():
                if fragment in line.lower():
                    try:
                        pid = int(line.strip().split()[0])
                        pids.append(pid)
                    except Exception:
                        continue
    except Exception:
        pass
    return pids


def _kill_pid(pid: int):
    try:
        if platform.system().lower().startswith("win"):
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        else:
            subprocess.run(
                ["kill", "-9", str(pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception:
        pass

def _terminate_process(proc) -> None:
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


def _watchdog_loop():
    global _posts_process, _accounts_process, _posts_started_at, _accounts_started_at
    while True:
        now = time.time()

        if _posts_process is not None and _posts_process.poll() is not None:
            _posts_process = None
            _posts_started_at = None
        if _accounts_process is not None and _accounts_process.poll() is not None:
            _accounts_process = None
            _accounts_started_at = None

        if _is_running(_posts_process) and _posts_started_at:
            if now - _posts_started_at >= RESTART_INTERVAL_SEC:
                _terminate_process(_posts_process)
                _posts_process = None
                _posts_started_at = None
                time.sleep(RESTART_PAUSE_SEC)
                start_posts_parser()

        if _is_running(_accounts_process) and _accounts_started_at:
            if now - _accounts_started_at >= RESTART_INTERVAL_SEC:
                _terminate_process(_accounts_process)
                _accounts_process = None
                _accounts_started_at = None
                time.sleep(RESTART_PAUSE_SEC)
                start_accounts_parser()

        if _posts_expected_running and _has_posts_tasks():
            if posts_parser_running():
                stale, reason = _health_stale(POSTS_HEALTH_PATH, _posts_started_at)
                if stale:
                    print(f"[watchdog] restart posts parser: {reason} at {_utc_now_iso()}")
                    control_parsers_action("restart_posts")
            else:
                print(f"[watchdog] start posts parser (down) at {_utc_now_iso()}")
                start_posts_parser()

        if _accounts_expected_running and _has_accounts_tasks():
            if accounts_parser_running():
                stale, reason = _health_stale(ACCOUNTS_HEALTH_PATH, _accounts_started_at)
                if stale:
                    print(f"[watchdog] restart accounts parser: {reason} at {_utc_now_iso()}")
                    control_parsers_action("restart_accounts")
            else:
                print(f"[watchdog] start accounts parser (down) at {_utc_now_iso()}")
                start_accounts_parser()

        time.sleep(5)


def _ensure_watchdog():
    global _watchdog_started
    if _watchdog_started:
        return
    _watchdog_started = True
    thread = threading.Thread(target=_watchdog_loop, daemon=True)
    thread.start()


def start_posts_parser() -> None:
    global _posts_process, _posts_started_at, _posts_expected_running, _posts_log_handle
    _posts_expected_running = True
    if _is_running(_posts_process) or _system_has_process(POSTS_PARSER_PATH):
        if _posts_started_at is None:
            _posts_started_at = time.time()
        _ensure_watchdog()
        return
    if not POSTS_PARSER_PATH.exists():
        return
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    cmd = _parser_python_cmd() + [str(POSTS_PARSER_PATH)]
    if _posts_log_handle is None or _posts_log_handle.closed:
        _posts_log_handle = _open_log_handle()
    _posts_process = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        stdout=_posts_log_handle,
        stderr=_posts_log_handle,
        env=env,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform.startswith("win") else 0,
    )
    _posts_started_at = time.time()
    _ensure_watchdog()


def stop_posts_parser() -> None:
    global _posts_process, _posts_started_at, _posts_expected_running, _posts_log_handle
    _posts_expected_running = False
    if _is_running(_posts_process):
        _terminate_process(_posts_process)
        _posts_process = None
        _posts_started_at = None
    for pid in _find_pids_for_script(POSTS_PARSER_PATH):
        _kill_pid(pid)
    _close_log_handle(_posts_log_handle)
    _posts_log_handle = None


def posts_parser_running() -> bool:
    return _is_running(_posts_process) or _system_has_process(POSTS_PARSER_PATH)


def restart_posts_parser() -> None:
    stop_posts_parser()
    time.sleep(1)
    start_posts_parser()


def start_accounts_parser() -> None:
    global _accounts_process, _accounts_started_at, _accounts_expected_running, _accounts_log_handle
    _accounts_expected_running = True
    if _is_running(_accounts_process) or _system_has_process(ACCOUNTS_PARSER_PATH):
        if _accounts_started_at is None:
            _accounts_started_at = time.time()
        _ensure_watchdog()
        return
    if not ACCOUNTS_PARSER_PATH.exists():
        return
    cmd = _parser_python_cmd() + [str(ACCOUNTS_PARSER_PATH)]
    if _accounts_log_handle is None or _accounts_log_handle.closed:
        _accounts_log_handle = _open_log_handle()
    _accounts_process = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        stdout=_accounts_log_handle,
        stderr=_accounts_log_handle,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform.startswith("win") else 0,
    )
    _accounts_started_at = time.time()
    _ensure_watchdog()


def stop_accounts_parser() -> None:
    global _accounts_process, _accounts_started_at, _accounts_expected_running, _accounts_log_handle
    _accounts_expected_running = False
    if _is_running(_accounts_process):
        _terminate_process(_accounts_process)
        _accounts_process = None
        _accounts_started_at = None
    for pid in _find_pids_for_script(ACCOUNTS_PARSER_PATH):
        _kill_pid(pid)
    _close_log_handle(_accounts_log_handle)
    _accounts_log_handle = None


def accounts_parser_running() -> bool:
    return _is_running(_accounts_process) or _system_has_process(ACCOUNTS_PARSER_PATH)


def restart_accounts_parser() -> None:
    stop_accounts_parser()
    time.sleep(1)
    start_accounts_parser()


def restart_parsers() -> None:
    restart_accounts_parser()
    restart_posts_parser()


def control_parsers_action(action: str) -> None:
    if action == "start_all":
        start_accounts_parser()
        start_posts_parser()
    elif action == "stop_all":
        stop_accounts_parser()
        stop_posts_parser()
    elif action == "restart_all":
        restart_parsers()
    elif action == "start_posts":
        start_posts_parser()
    elif action == "stop_posts":
        stop_posts_parser()
    elif action == "restart_posts":
        restart_posts_parser()
    elif action == "start_accounts":
        start_accounts_parser()
    elif action == "stop_accounts":
        stop_accounts_parser()
    elif action == "restart_accounts":
        restart_accounts_parser()


def normalize_account(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("@"):
        value = value[1:]
    return f"https://www.threads.com/@{value}"

