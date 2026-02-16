import json
import os
import tempfile
from pathlib import Path
from typing import Callable, Optional

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from json_file_lock import path_lock

BASE_DIR = Path(__file__).resolve().parent
USERS_DIR = BASE_DIR / "json bd"
PAGES_DIR = BASE_DIR / "pages"
LOGIN_PAGE = PAGES_DIR / "login.html"
HOME_PAGE = PAGES_DIR / "home.html"

auth_router = APIRouter()


def is_logged_in(request: Request) -> bool:
    return request.session.get("logged_in") is True


def _read_user_file(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError:
        return None


def _find_user_path(username: str) -> Optional[Path]:
    direct_path = USERS_DIR / f"{username}.json"
    if direct_path.exists():
        return direct_path
    if not USERS_DIR.exists():
        return None
    for path in USERS_DIR.glob("*.json"):
        data = _read_user_file(path)
        if data and data.get("username") == username:
            return path
    return None


def load_user_data(username: str) -> Optional[dict]:
    user_path = _find_user_path(username)
    if not user_path:
        return None
    return _read_user_file(user_path)


def _write_user_file(user_path: Path, data: dict, username: str) -> None:
    data["username"] = username
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{user_path.name}.",
        suffix=".tmp",
        dir=str(user_path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(tmp_name, user_path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except OSError:
            pass


def save_user_data(username: str, data: dict) -> None:
    user_path = _find_user_path(username) or (USERS_DIR / f"{username}.json")
    with path_lock(user_path):
        _write_user_file(user_path, data, username)


def update_user_data(username: str, mutator: Callable[[dict], None]) -> dict:
    user_path = _find_user_path(username) or (USERS_DIR / f"{username}.json")
    with path_lock(user_path):
        if user_path.exists():
            current = _read_user_file(user_path)
            if not isinstance(current, dict):
                current = {"username": username}
        else:
            current = {"username": username}
        mutator(current)
        _write_user_file(user_path, current, username)
        return current


def ensure_threads_lists(data: dict) -> dict:
    data.setdefault("accounts", [])
    data.setdefault("posts", [])
    data.setdefault("seen_posts", [])
    data.setdefault("auto_add_latest", {})
    return data


def validate_user(username: str, password: str) -> bool:
    user = load_user_data(username)
    return user is not None and user.get("password") == password


def _unauthorized_json() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)


@auth_router.get("/", name="auth.index")
async def index(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(HOME_PAGE)


@auth_router.get("/login", name="auth.login_page")
async def login_page(request: Request):
    if is_logged_in(request):
        return RedirectResponse(url="/", status_code=303)
    return FileResponse(LOGIN_PAGE)


@auth_router.post("/api/login", name="auth.login")
async def login(request: Request):
    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", "")).strip()

    user_ok = username and password and validate_user(username, password)

    if user_ok:
        request.session["logged_in"] = True
        request.session["who"] = username
        return {"ok": True}

    return {"ok": False, "error": "Неверные данные для входа."}


@auth_router.get("/api/me", name="auth.me")
async def me(request: Request):
    if not is_logged_in(request):
        return _unauthorized_json()
    return {"ok": True, "who": request.session.get("who", "")}


@auth_router.post("/api/logout", name="auth.api_logout")
async def api_logout(request: Request):
    request.session.clear()
    return {"ok": True}


@auth_router.post("/logout", name="auth.logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
