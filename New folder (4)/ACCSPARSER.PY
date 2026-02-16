import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import unquote

from json_file_lock import path_lock

try:
    from curl_cffi.requests import Session
except ImportError as exc:  # pragma: no cover - runtime dependency
    raise SystemExit(
        "curl_cffi is not installed. Install it with:\n"
        "  pip install curl_cffi\n"
    ) from exc


BASE_DIR = Path(__file__).resolve().parent
USERS_DIR = BASE_DIR / "json bd"
RUNTIME_DIR = BASE_DIR / "runtime"
HEALTH_PATH = RUNTIME_DIR / "parser_health_accounts.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ACCEPT_LANGUAGE = "en-US,en;q=0.9"

COOKIE_NAME = "_js_ig_did"
COOKIE_PATTERN = re.compile(r'"_js_ig_did"\s*:\s*\{\s*"value"\s*:\s*"([^"]+)"')
FOLLOWERS_PATTERN = re.compile(r'<span[^>]*title="([0-9][0-9.,\s]*)"')

BATCH_SIZE = 10
PER_LINK_DELAY = 1.5
BATCH_DELAY = 5
CYCLE_DELAY = 30
REQUEST_TIMEOUT_SEC = 60
EVENTS_ENDPOINT = os.environ.get("PARSER_EVENTS_URL", "http://127.0.0.1:8000/internal/parser-events").strip()
EVENTS_TIMEOUT_SEC = int(os.environ.get("PARSER_EVENTS_TIMEOUT_SEC", "15"))
EVENTS_TOKEN = os.environ.get("PARSER_INTERNAL_TOKEN", "").strip()


def _health_update(**kwargs) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    payload = {}
    try:
        payload = json.loads(HEALTH_PATH.read_text(encoding="utf-8-sig"))
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}
    payload.update(kwargs)
    payload["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{HEALTH_PATH.name}.",
        suffix=".tmp",
        dir=str(HEALTH_PATH.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
        os.replace(tmp_name, HEALTH_PATH)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except OSError:
            pass


def _load_users():
    users = []
    if not USERS_DIR.exists():
        return users
    for path in USERS_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        if isinstance(data, dict):
            users.append((path, data))
    return users


def _save_user(path: Path, data: dict) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f"{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.remove(tmp_name)
        except OSError:
            pass


def _send_stats_to_backend(stats_by_url: dict) -> bool:
    if not isinstance(stats_by_url, dict) or not stats_by_url:
        return True

    payload = {
        "type": "accounts_stats_batch",
        "stats_by_url": stats_by_url,
        "sent_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if EVENTS_TOKEN:
        headers["X-Parser-Token"] = EVENTS_TOKEN

    request = Request(EVENTS_ENDPOINT, data=raw, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=EVENTS_TIMEOUT_SEC) as response:
            status_code = getattr(response, "status", 200)
            if status_code >= 300:
                print(f"[accounts-parser] backend status: {status_code}")
                return False
        return True
    except HTTPError as exc:
        print(f"[accounts-parser] backend HTTP error: {exc.code}")
        return False
    except URLError as exc:
        print(f"[accounts-parser] backend URL error: {exc.reason}")
        return False
    except Exception as exc:
        print(f"[accounts-parser] backend push failed: {exc}")
        return False


def _ensure_stats(data: dict) -> dict:
    stats = data.setdefault("stats", {})
    stats.setdefault("accounts", {})
    stats.setdefault("posts", {})
    return stats


def normalize_account(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("@"):
        value = value[1:]
    return f"https://www.threads.com/@{value}"


def extract_cookie_value(html):
    match = COOKIE_PATTERN.search(html)
    return match.group(1) if match else None


def extract_followers_count(html):
    match = FOLLOWERS_PATTERN.search(html)
    if not match:
        return None
    raw = match.group(1)
    digits = re.sub(r"[^\d]", "", raw)
    if not digits:
        return None
    return int(digits)


def build_post_url(nickname, post_id):
    return f"https://www.threads.com/@{nickname}/post/{post_id}"


def _format_datetime(raw: str) -> dict:
    if not raw:
        return {"raw": ""}
    value = unquote(raw)
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except Exception:
        return {"raw": value}
    msk_tz = timezone(timedelta(hours=3))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=msk_tz)
    msk = parsed.astimezone(msk_tz)
    return {
        "raw": value,
        "msk": msk.isoformat(),
        "msk_human": msk.strftime("%Y-%m-%d %H:%M:%S MSK"),
    }


def extract_post_items(html: str, nickname: str, limit: int):
    if not html:
        return []
    href_pattern = re.compile(r'href="/@{}/post/([^"?#/]+)"'.format(re.escape(nickname)))
    date_pattern = re.compile(r'dateTime="([^"]+)"')
    matches = list(href_pattern.finditer(html))
    items = []
    seen_ids = set()
    for idx, match in enumerate(matches):
        post_id = match.group(1)
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)
        start = match.end()
        if idx + 1 < len(matches):
            end = matches[idx + 1].start()
        else:
            end = min(len(html), start + 8000)
        segment = html[start:end]
        dt_match = date_pattern.search(segment)
        dt = _format_datetime(dt_match.group(1)) if dt_match else {"raw": ""}
        items.append({"url": build_post_url(nickname, post_id), "dateTime": dt})
        if len(items) >= limit:
            break
    return items


def fetch_html(session: Session, url: str, cookie_value: str | None = None):
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": ACCEPT_LANGUAGE,
    }
    cookies = {COOKIE_NAME: cookie_value} if cookie_value else None
    _health_update(last_request_ts_utc=datetime.now(timezone.utc).isoformat(), last_request_url=url)
    try:
        response = session.get(
            url,
            headers=headers,
            cookies=cookies,
            timeout=REQUEST_TIMEOUT_SEC,
        )
        _health_update(
            last_response_ts_utc=datetime.now(timezone.utc).isoformat(),
            last_response_url=url,
            last_response_status=getattr(response, "status_code", None),
        )
        if response is None:
            return ""
        text = response.text or ""
        if text:
            return text
        content = response.content or b""
        return content.decode("utf-8", errors="ignore")
    except Exception as exc:
        _health_update(
            last_error_ts_utc=datetime.now(timezone.utc).isoformat(),
            last_error=str(exc),
        )
        print(f"Request error for {url}: {exc}")
        return ""


def fetch_profile_html(session: Session, nickname: str):
    url = f"https://www.threads.com/@{nickname}"
    first_html = fetch_html(session, url)
    cookie_value = extract_cookie_value(first_html)
    second_html = first_html
    if cookie_value:
        fetched = fetch_html(session, url, cookie_value=cookie_value)
        if fetched:
            second_html = fetched
    return second_html


def process_nickname(session: Session, nickname: str, posts_per_profile=4, show_links=True):
    html = fetch_profile_html(session, nickname)

    followers = extract_followers_count(html)
    if followers is not None:
        print(f"Followers (from <span title>): {followers}")
    else:
        print("Followers not found in <span title>")

    post_items = extract_post_items(html, nickname, posts_per_profile)

    if post_items:
        if show_links:
            for item in post_items:
                print(f"Post link: {item.get('url')}")
        print(f"Parsed posts count: {len(post_items)}")
    else:
        print("Post links not found")
        print("Parsed posts count: 0")

    return {
        "followers": followers,
        "posts": post_items,
        "latest_post": post_items[0] if post_items else {"url": "", "dateTime": {"raw": ""}},
        "nickname": nickname,
    }


def nickname_from_url(url):
    match = re.search(r"/@([^/?#]+)", url)
    return match.group(1) if match else None


def _collect_accounts(users):
    mapping = {}
    for path, data in users:
        accounts = data.get("accounts", []) if isinstance(data, dict) else []
        for item in accounts:
            key = normalize_account(str(item))
            if not key:
                continue
            mapping.setdefault(key, set()).add(path)
    return mapping


def _update_users_accounts_stats(users, stats_by_url):
    for path, data in users:
        with path_lock(path):
            current = data
            try:
                fresh = json.loads(path.read_text(encoding="utf-8-sig"))
                if isinstance(fresh, dict):
                    current = fresh
            except Exception:
                pass
            stats = _ensure_stats(current).get("accounts", {})
            for item in current.get("accounts", []) if isinstance(current, dict) else []:
                key = normalize_account(str(item))
                if key in stats_by_url:
                    payload = stats_by_url[key]
                    prev_stats = stats.get(key, {}) if isinstance(stats.get(key), dict) else {}
                    if isinstance(payload, dict):
                        payload = dict(payload)
                        if "followers_history" in prev_stats:
                            payload["followers_history"] = prev_stats.get("followers_history")
                        if "last_post_tracking" in prev_stats:
                            payload["last_post_tracking"] = prev_stats.get("last_post_tracking")
                        if "post_history" in prev_stats:
                            payload["post_history"] = prev_stats.get("post_history")
                        if "pending_post_history" in prev_stats:
                            payload["pending_post_history"] = prev_stats.get("pending_post_history")
                    stats[key] = payload
            _save_user(path, current)


def run_once(session: Session, urls, per_link_delay, show_links=True):
    total = 0
    stats_by_url = {}
    for url in urls:
        nick = nickname_from_url(url)
        if not nick:
            count = 0
        else:
            try:
                stats = process_nickname(session, nick, posts_per_profile=4, show_links=show_links)
                count = len(stats.get("posts", []))
                stats_by_url[url] = stats
            except Exception as e:
                print(f"Error processing {url}: {e}")
                count = 0
        total += count
        if per_link_delay > 0:
            time.sleep(per_link_delay)
    print(f"Total parsed posts: {total}")
    return stats_by_url


def main():
    test_mode = "--test" in sys.argv
    _health_update(
        parser="accounts",
        pid=os.getpid(),
        started_at_utc=datetime.now(timezone.utc).isoformat(),
    )

    with Session(impersonate="chrome142") as session:
        if test_mode:
            users = _load_users()
            account_map = _collect_accounts(users)
            urls = list(account_map.keys())
            if not urls:
                print("No URLs in user JSON files")
                return
            stats_by_url = run_once(session, urls, PER_LINK_DELAY, show_links=True)
            _send_stats_to_backend(stats_by_url)
            return

        while True:
            users = _load_users()
            account_map = _collect_accounts(users)
            urls = list(account_map.keys())
            if not urls:
                print("No URLs in user JSON files")
                time.sleep(CYCLE_DELAY)
                continue

            total = len(urls)
            batches = [urls[i:i + BATCH_SIZE] for i in range(0, total, BATCH_SIZE)]
            stats_by_url = {}

            for idx, batch in enumerate(batches, start=1):
                for url in batch:
                    nick = nickname_from_url(url)
                    if not nick:
                        continue
                    try:
                        stats = process_nickname(session, nick, posts_per_profile=4, show_links=True)
                        stats_by_url[url] = stats
                    except Exception as e:
                        print(f"Error processing {url}: {e}")
                    time.sleep(PER_LINK_DELAY)

                if total >= BATCH_SIZE and idx < len(batches):
                    time.sleep(BATCH_DELAY)

            _send_stats_to_backend(stats_by_url)
            print(f"Total parsed posts: {len(stats_by_url)}")
            time.sleep(CYCLE_DELAY)


if __name__ == "__main__":
    main()
