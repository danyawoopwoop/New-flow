from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from auth import is_logged_in, load_user_data, ensure_threads_lists, update_user_data
from parsers_bridge import (
    accounts_parser_running,
    posts_parser_running,
    normalize_account,
    control_parsers_action,
)

threads_router = APIRouter()
BASE_DIR = Path(__file__).resolve().parent
PAGES_DIR = BASE_DIR / "pages"

THREADS_PAGE = PAGES_DIR / "threads.html"
THREADS_ACCOUNTS_PAGE = PAGES_DIR / "threads_accounts.html"
THREADS_POSTS_PAGE = PAGES_DIR / "threads_posts.html"
THREADS_ACCOUNT_STATS_PAGE = PAGES_DIR / "threads_account_stats.html"
THREADS_POST_STATS_PAGE = PAGES_DIR / "threads_post_stats.html"
THREADS_HISTORY_PAGE = PAGES_DIR / "threads_history.html"
THREADS_HISTORY_POST_PAGE = PAGES_DIR / "threads_history_post.html"
THREADS_PARSERS_PAGE = PAGES_DIR / "threads_parsers.html"

MSK_TZ = timezone(timedelta(hours=3))


def _ensure_stats(data: dict) -> dict:
    stats = data.setdefault("stats", {})
    stats.setdefault("accounts", {})
    stats.setdefault("posts", {})
    return stats


def _parse_ts(value: str):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def _parse_post_time(dt_value):
    if isinstance(dt_value, dict):
        raw = dt_value.get("raw") or dt_value.get("msk") or ""
        if not raw:
            raw = dt_value.get("msk_human") or ""
    else:
        raw = str(dt_value) if dt_value else ""
    raw = raw.strip()
    if not raw:
        return None
    if raw.endswith(" MSK"):
        try:
            base = raw.replace(" MSK", "")
            parsed = datetime.strptime(base, "%Y-%m-%d %H:%M:%S")
            return parsed.replace(tzinfo=timezone(timedelta(hours=3))).astimezone(timezone.utc)
        except Exception:
            return None
    return _parse_ts(raw)


def _dt_has_value(dt_value) -> bool:
    if isinstance(dt_value, dict):
        return bool(dt_value.get("msk_human") or dt_value.get("msk") or dt_value.get("raw"))
    if dt_value is None:
        return False
    return bool(str(dt_value).strip())


def _dt_to_payload(dt_value):
    if isinstance(dt_value, dict):
        return {
            "raw": dt_value.get("raw", ""),
            "msk": dt_value.get("msk", ""),
            "msk_human": dt_value.get("msk_human", ""),
        }
    if dt_value is None:
        return {"raw": "", "msk": "", "msk_human": ""}
    raw = str(dt_value).strip()
    if not raw:
        return {"raw": "", "msk": "", "msk_human": ""}
    parsed = _parse_post_time(raw)
    if not parsed:
        return {"raw": raw, "msk": "", "msk_human": ""}
    msk = parsed.astimezone(MSK_TZ)
    return {
        "raw": raw,
        "msk": msk.isoformat(),
        "msk_human": msk.strftime("%Y-%m-%d %H:%M:%S MSK"),
    }


def _resolve_post_date_payload(data: dict, item: str, item_stats):
    date_value = item_stats.get("dateTime") if isinstance(item_stats, dict) else None
    if _dt_has_value(date_value):
        return _dt_to_payload(date_value)

    accounts_stats = _ensure_stats(data).get("accounts", {})
    if isinstance(accounts_stats, dict):
        for acc_stats in accounts_stats.values():
            if not isinstance(acc_stats, dict):
                continue
            latest = acc_stats.get("latest_post")
            if isinstance(latest, dict):
                latest_url = (latest.get("url") or latest.get("link") or "").strip()
                if latest_url == item and _dt_has_value(latest.get("dateTime")):
                    return _dt_to_payload(latest.get("dateTime"))
            posts = acc_stats.get("posts", [])
            if isinstance(posts, list):
                for entry in posts:
                    if isinstance(entry, dict):
                        url = (entry.get("url") or entry.get("link") or "").strip()
                        dt = entry.get("dateTime")
                    else:
                        url = str(entry).strip()
                        dt = None
                    if url == item and _dt_has_value(dt):
                        return _dt_to_payload(dt)
            post_history = acc_stats.get("post_history", {})
            if isinstance(post_history, dict):
                history_entry = post_history.get(item)
                if isinstance(history_entry, dict):
                    history_dt = history_entry.get("post_time_utc")
                    if _dt_has_value(history_dt):
                        return _dt_to_payload(history_dt)

    return {"raw": "", "msk": "", "msk_human": ""}


def _format_hour_range(start_ts: datetime, end_ts: datetime) -> str:
    start_msk = start_ts.astimezone(MSK_TZ)
    end_msk = end_ts.astimezone(MSK_TZ)
    return f"{start_msk:%H:%M}–{end_msk:%H:%M}"


def _compute_hourly(history_rows, start_ts: datetime, hours: int, base_followers):
    if not history_rows:
        history_rows = []
    idx = 0
    prev_value = base_followers
    while idx < len(history_rows) and history_rows[idx][0] <= start_ts:
        prev_value = history_rows[idx][1]
        idx += 1
    if prev_value is None:
        prev_value = history_rows[0][1] if history_rows else 0

    results = []
    for hour in range(1, hours + 1):
        end_ts = start_ts + timedelta(hours=hour)
        value = prev_value
        while idx < len(history_rows) and history_rows[idx][0] <= end_ts:
            value = history_rows[idx][1]
            idx += 1
        delta = value - prev_value if prev_value is not None else 0
        results.append(
            {
                "hour": hour,
                "delta": delta,
                "range": _format_hour_range(end_ts - timedelta(hours=1), end_ts),
            }
        )
        prev_value = value
    return results


def _history_rows_from_stats(item_stats):
    history = item_stats.get("followers_history", []) if isinstance(item_stats, dict) else []
    rows = []
    for entry in history if isinstance(history, list) else []:
        ts = _parse_ts(entry.get("ts_utc", ""))
        followers = entry.get("followers")
        if ts and isinstance(followers, int):
            rows.append((ts, followers))
    rows.sort(key=lambda x: x[0])
    return rows


def _unauthorized_json() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)


def _collect_parser_status():
    base = Path(__file__).resolve().parent / "json bd"
    posts_tasks = []
    accounts_tasks = []
    posts_ok = True
    accounts_ok = True

    if base.exists():
        for path in base.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8-sig"))
            except Exception:
                continue
            posts = data.get("posts") or []
            accounts = data.get("accounts") or []
            stats = data.get("stats") or {}
            post_stats = stats.get("posts") or {}
            acc_stats = stats.get("accounts") or {}

            for p in posts:
                posts_tasks.append(p)
                if p not in post_stats or not isinstance(post_stats.get(p), dict) or not post_stats.get(p):
                    posts_ok = False

            for a in accounts:
                accounts_tasks.append(a)
                key = normalize_account(str(a))
                entry = acc_stats.get(key) if key else None
                if not entry or not isinstance(entry, dict) or not entry:
                    accounts_ok = False

    has_tasks = bool(posts_tasks or accounts_tasks)
    posts_running = posts_parser_running()
    accounts_running = accounts_parser_running()

    overall_ok = True
    if posts_tasks and (not posts_running or not posts_ok):
        overall_ok = False
    if accounts_tasks and (not accounts_running or not accounts_ok):
        overall_ok = False

    if not has_tasks:
        status = "Нет задач"
        color = "idle"
    elif overall_ok:
        status = "Работает"
        color = "ok"
    else:
        status = "Не работает"
        color = "bad"

    return {
        "text": status,
        "color": color,
        "has_tasks": has_tasks,
        "posts_running": posts_running,
        "accounts_running": accounts_running,
        "posts_ok": posts_ok,
        "accounts_ok": accounts_ok,
    }


def _require_user(request: Request):
    if not is_logged_in(request):
        return None, _unauthorized_json()
    return request.session.get("who", ""), None


def _build_account_stats_payload(username: str, item: str):
    data = load_user_data(username) or {"username": username}
    ensure_threads_lists(data)
    data.setdefault("auto_add_latest", {})
    stats = _ensure_stats(data).get("accounts", {})
    key = normalize_account(item)
    item_stats = stats.get(key, {})
    tracked_posts = set(data.get("posts", []))
    seen_posts = data.get("seen_posts", [])
    normalized_posts = []
    followers_dynamics = []
    followers_since_post = {}
    post_hourly = []
    post_hourly_options = []
    updated = False

    def _normalize_dt(value):
        if isinstance(value, dict):
            return value
        if value:
            return {"raw": str(value)}
        return {}

    for entry in item_stats.get("posts", []) or []:
        if isinstance(entry, dict):
            url = (entry.get("url") or entry.get("link") or "").strip()
            dt = _normalize_dt(entry.get("dateTime"))
        else:
            url = str(entry).strip()
            dt = {}
        if url:
            normalized_posts.append({"url": url, "dateTime": dt})

    latest_entry = item_stats.get("latest_post")
    if isinstance(latest_entry, dict):
        latest_url = (latest_entry.get("url") or latest_entry.get("link") or "").strip()
        latest_dt = _normalize_dt(latest_entry.get("dateTime"))
    elif isinstance(latest_entry, str):
        latest_url = latest_entry.strip()
        latest_dt = {}
    else:
        latest_url = ""
        latest_dt = {}
    latest_post = {"url": latest_url, "dateTime": latest_dt}

    history = item_stats.get("followers_history", []) if isinstance(item_stats, dict) else []
    history_rows = _history_rows_from_stats(item_stats)
    current_followers = item_stats.get("followers") if isinstance(item_stats, dict) else None
    now_dt = datetime.now(timezone.utc)
    now_ts = now_dt.isoformat()

    if isinstance(current_followers, int):
        if not isinstance(history, list):
            history = []
        last_entry = history[-1] if history else {}
        last_ts = _parse_ts(last_entry.get("ts_utc", "")) if isinstance(last_entry, dict) else None
        last_value = last_entry.get("followers") if isinstance(last_entry, dict) else None
        should_append = False
        if last_ts is None:
            should_append = True
        else:
            delta_minutes = (now_dt - last_ts).total_seconds() / 60
            if delta_minutes >= 5:
                should_append = True
        if last_value is None or last_value != current_followers:
            should_append = True
        if should_append:
            history.append({"ts_utc": now_ts, "followers": current_followers})
            cutoff = now_dt - timedelta(days=8)
            trimmed = []
            for entry in history:
                ts = _parse_ts(entry.get("ts_utc", "")) if isinstance(entry, dict) else None
                if ts and ts >= cutoff:
                    trimmed.append(entry)
            history = trimmed
            item_stats["followers_history"] = history
            updated = True
        history_rows = _history_rows_from_stats(item_stats)

    tracking = item_stats.get("last_post_tracking", {}) if isinstance(item_stats, dict) else {}
    if isinstance(item_stats, dict):
        current_latest_url = ""
        if isinstance(item_stats.get("latest_post"), dict):
            current_latest_url = (
                item_stats.get("latest_post", {}).get("url")
                or item_stats.get("latest_post", {}).get("link")
                or ""
            ).strip()
        elif isinstance(item_stats.get("latest_post"), str):
            current_latest_url = item_stats.get("latest_post").strip()
        tracking_url = ""
        if isinstance(tracking, dict):
            tracking_url = (tracking.get("post_url") or "").strip()
        if current_latest_url and current_latest_url != tracking_url and isinstance(current_followers, int):
            prev_tracking = tracking if isinstance(tracking, dict) else {}
            prev_url = (prev_tracking.get("post_url") or "").strip()
            prev_time = _parse_ts(prev_tracking.get("post_time_utc", "")) if prev_tracking else None
            if not prev_time and prev_url:
                for post_item in normalized_posts:
                    if post_item.get("url") == prev_url:
                        prev_time = _parse_post_time(post_item.get("dateTime"))
                        break
            pending = item_stats.get("pending_post_history", [])
            if not isinstance(pending, list):
                pending = []
            if prev_url and prev_time:
                already_pending = any(
                    isinstance(p, dict) and p.get("post_url") == prev_url for p in pending
                )
                post_history = item_stats.get("post_history", {})
                if not isinstance(post_history, dict):
                    post_history = {}
                if prev_url not in post_history and not already_pending:
                    pending.append(
                        {
                            "post_url": prev_url,
                            "post_time_utc": prev_time.isoformat(),
                            "followers_at_post": prev_tracking.get("followers_at_post"),
                        }
                    )
                    item_stats["pending_post_history"] = pending
                    updated = True
            tracked_time = _parse_post_time(latest_dt)
            item_stats["last_post_tracking"] = {
                "post_url": current_latest_url,
                "followers_at_post": current_followers,
                "ts_utc": now_ts,
                "post_time_utc": tracked_time.isoformat() if tracked_time else "",
            }
            tracking = item_stats["last_post_tracking"]
            updated = True

    if history_rows:
        current_ts, current_followers = history_rows[-1]
        intervals = [
            ("15 минут", 15 * 60),
            ("30 минут", 30 * 60),
            ("1 час", 60 * 60),
            ("3 часа", 3 * 60 * 60),
            ("6 часов", 6 * 60 * 60),
            ("12 часов", 12 * 60 * 60),
            ("24 часа", 24 * 60 * 60),
            ("3 дня", 3 * 24 * 60 * 60),
            ("7 дней", 7 * 24 * 60 * 60),
        ]
        for label, seconds in intervals:
            target = current_ts - timedelta(seconds=seconds)
            past_value = None
            for ts, followers in reversed(history_rows):
                if ts <= target:
                    past_value = followers
                    break
            delta = (current_followers - past_value) if past_value is not None else 0
            followers_dynamics.append({"label": label, "delta": delta})
        if isinstance(tracking, dict):
            base = tracking.get("followers_at_post")
            if isinstance(base, int):
                followers_since_post = {
                    "delta": current_followers - base,
                    "post_url": tracking.get("post_url", ""),
                }

    base_followers = None
    if isinstance(tracking, dict):
        base_followers = tracking.get("followers_at_post")

    tracked_url = tracking.get("post_url") if isinstance(tracking, dict) else ""
    tracked_time = _parse_ts(tracking.get("post_time_utc", "")) if isinstance(tracking, dict) else None
    if not tracked_time and tracked_url:
        for post_item in normalized_posts:
            if post_item.get("url") == tracked_url:
                tracked_time = _parse_post_time(post_item.get("dateTime"))
                break

    if tracked_url and tracked_time:
        post_hourly = _compute_hourly(history_rows, tracked_time, 24, base_followers)
        post_hourly_options = [
            {"range": row.get("range"), "delta": row.get("delta", 0)}
            for row in post_hourly
        ]
        if now_dt - tracked_time >= timedelta(hours=24):
            post_history = item_stats.get("post_history", {})
            if not isinstance(post_history, dict):
                post_history = {}
            entry = post_history.get(tracked_url, {})
            entry = entry if isinstance(entry, dict) else {}
            post_time_iso = tracked_time.isoformat()
            if entry.get("post_time_utc") != post_time_iso:
                entry = {
                    "post_url": tracked_url,
                    "post_time_utc": post_time_iso,
                    "followers_at_post": base_followers,
                    "account": key,
                }
            hourly_map = entry.get("hourly", {})
            if not isinstance(hourly_map, dict):
                hourly_map = {}
            hourly_map["24"] = post_hourly
            entry["hourly"] = hourly_map
            entry["last_updated_utc"] = now_ts
            post_history[tracked_url] = entry
            item_stats["post_history"] = post_history
            updated = True

    pending = item_stats.get("pending_post_history", [])
    if not isinstance(pending, list):
        pending = []
    if pending:
        post_history = item_stats.get("post_history", {})
        if not isinstance(post_history, dict):
            post_history = {}
        still_pending = []
        for entry in pending:
            if not isinstance(entry, dict):
                continue
            url = (entry.get("post_url") or "").strip()
            post_time = _parse_ts(entry.get("post_time_utc", ""))
            if not url or not post_time:
                continue
            if now_dt - post_time < timedelta(hours=24):
                still_pending.append(entry)
                continue
            base = entry.get("followers_at_post")
            hourly = _compute_hourly(history_rows, post_time, 24, base)
            history_entry = post_history.get(url, {})
            history_entry = history_entry if isinstance(history_entry, dict) else {}
            post_time_iso = post_time.isoformat()
            if history_entry.get("post_time_utc") != post_time_iso:
                history_entry = {
                    "post_url": url,
                    "post_time_utc": post_time_iso,
                    "followers_at_post": base,
                    "account": key,
                }
            hourly_map = history_entry.get("hourly", {})
            if not isinstance(hourly_map, dict):
                hourly_map = {}
            hourly_map["24"] = hourly
            history_entry["hourly"] = hourly_map
            history_entry["last_updated_utc"] = now_ts
            post_history[url] = history_entry
            updated = True
        item_stats["post_history"] = post_history
        item_stats["pending_post_history"] = still_pending

    posts_list = data.get("posts", [])
    auto_map = data.get("auto_add_latest", {}) or {}
    auto_enabled = auto_map.get(key, True)

    if isinstance(item_stats, dict):
        posts_pool = item_stats.get("posts", []) if isinstance(item_stats.get("posts", []), list) else []
        first_url = ""
        for post_entry in posts_pool:
            if isinstance(post_entry, dict):
                post_url = (post_entry.get("url") or post_entry.get("link") or "").strip()
            else:
                post_url = str(post_entry).strip()
            if not post_url:
                continue
            first_url = post_url
            break
        if first_url and auto_enabled:
            if first_url not in seen_posts:
                seen_posts.append(first_url)
                updated = True
            if first_url not in posts_list:
                posts_list.append(first_url)
                updated = True
            tracked_posts.add(first_url)

    if updated:
        def _mutate(current: dict):
            ensure_threads_lists(current)
            current_stats = _ensure_stats(current).get("accounts", {})
            current_stats[key] = item_stats
            current["seen_posts"] = list(seen_posts)
            current["posts"] = list(posts_list)

        update_user_data(username, _mutate)

    return {
        "who": username,
        "item": item,
        "stats": item_stats,
        "posts": normalized_posts,
        "latest_post": latest_post,
        "tracked_posts": list(tracked_posts),
        "followers_dynamics": followers_dynamics,
        "followers_since_post": followers_since_post,
        "post_hourly": post_hourly,
        "post_hourly_options": post_hourly_options,
    }


def _build_post_stats_payload(username: str, item: str):
    data = load_user_data(username) or {"username": username}
    ensure_threads_lists(data)
    stats = _ensure_stats(data).get("posts", {})
    item_stats = stats.get(item, {})
    post_date = _resolve_post_date_payload(data, item, item_stats)
    dynamics = {}
    tracking = item_stats.get("tracking", {}) if isinstance(item_stats, dict) else {}
    timeline = tracking.get("timeline", []) if isinstance(tracking, dict) else []
    try:
        timeline = sorted(
            timeline,
            key=lambda p: datetime.fromisoformat(p.get("ts_utc", ""))
            if p.get("ts_utc")
            else datetime.min.replace(tzinfo=timezone.utc),
        )
    except Exception:
        pass
    if timeline:
        metrics = ["views", "likes", "comments", "repost", "shared"]
        intervals = [
            ("За последние 15 минут", 15 * 60),
            ("За последние 30 минут", 30 * 60),
            ("За последний 1 час", 60 * 60),
            ("За последние 3 часа", 3 * 60 * 60),
            ("За последние 6 часов", 6 * 60 * 60),
            ("За последние 12 часов", 12 * 60 * 60),
            ("За последние 24 часа", 24 * 60 * 60),
        ]
        now_dt = (
            datetime.fromisoformat(timeline[-1].get("ts_utc"))
            if timeline[-1].get("ts_utc")
            else datetime.now(timezone.utc)
        )

        def find_value_at(target_dt, metric):
            val = None
            for point in reversed(timeline):
                ts = point.get("ts_utc")
                try:
                    ts_dt = datetime.fromisoformat(ts)
                except Exception:
                    continue
                if ts_dt <= target_dt:
                    val = point.get(metric)
                    break
            if val is None:
                val = timeline[0].get(metric)
            return val

        for metric in metrics:
            current = timeline[-1].get(metric)
            cards = []
            for label, seconds in intervals:
                past_dt = now_dt - timedelta(seconds=seconds)
                past_val = find_value_at(past_dt, metric)
                if isinstance(current, int) and isinstance(past_val, int):
                    delta = current - past_val
                else:
                    delta = 0
                cards.append({"label": label, "delta": delta})
            dynamics[metric] = cards

    return {
        "who": username,
        "item": item,
        "stats": item_stats,
        "post_date": post_date,
        "dynamics": dynamics,
    }


def _metric_label(metric: str) -> str:
    labels = {
        "views": "Просмотры",
        "likes": "Лайки",
        "comments": "Комментарии",
        "repost": "Репосты",
        "shared": "Поделились",
    }
    return labels.get(metric, metric)


def _post_history_payload(item_stats: dict):
    tracking = item_stats.get("tracking", {}) if isinstance(item_stats, dict) else {}
    history = tracking.get("history_24h", {}) if isinstance(tracking, dict) else {}
    if not isinstance(history, dict):
        history = {}
    metrics = history.get("metrics", {})
    if not isinstance(metrics, dict):
        metrics = {}

    metric_options = []
    hourly_by_metric = {}
    for key in ["views", "likes", "comments", "repost", "shared"]:
        rows = metrics.get(key, [])
        if not isinstance(rows, list):
            rows = []
        normalized = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            normalized.append(
                {
                    "hour": row.get("hour"),
                    "range": row.get("range", ""),
                    "delta": row.get("delta", 0),
                }
            )
        hourly_by_metric[key] = normalized
        metric_options.append({"key": key, "label": _metric_label(key)})

    started_at = history.get("start_ts_utc") or tracking.get("started_at_utc") or ""
    completed_at = history.get("completed_at_utc") or ""
    started_dt = _parse_ts(started_at)
    completed_dt = _parse_ts(completed_at)

    return {
        "finalized": bool(history.get("finalized")),
        "ready_hours": int(history.get("ready_hours") or 0),
        "metric_options": metric_options,
        "hourly_by_metric": hourly_by_metric,
        "started_at": started_at,
        "completed_at": completed_at,
        "started_at_human": started_dt.astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M MSK") if started_dt else "",
        "completed_at_human": completed_dt.astimezone(MSK_TZ).strftime("%Y-%m-%d %H:%M MSK") if completed_dt else "",
    }

@threads_router.get("/threads", name="threads.threads_home")
async def threads_home(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(THREADS_PAGE)


@threads_router.get("/threads/accounts", name="threads.threads_accounts")
async def threads_accounts(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(THREADS_ACCOUNTS_PAGE)


@threads_router.get("/threads/posts", name="threads.threads_posts")
async def threads_posts(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(THREADS_POSTS_PAGE)


@threads_router.get("/threads/accounts/stats", name="threads.threads_accounts_stats")
async def threads_accounts_stats(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    item = str(request.query_params.get("item", "")).strip()
    if not item:
        return RedirectResponse(url="/threads/accounts", status_code=303)
    return FileResponse(THREADS_ACCOUNT_STATS_PAGE)


@threads_router.get("/threads/posts/stats", name="threads.threads_posts_stats")
async def threads_posts_stats(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    item = str(request.query_params.get("item", "")).strip()
    if not item:
        return RedirectResponse(url="/threads/posts", status_code=303)
    return FileResponse(THREADS_POST_STATS_PAGE)


@threads_router.get("/threads/history", name="threads.threads_history")
async def threads_history(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(THREADS_HISTORY_PAGE)


@threads_router.get("/threads/history/post", name="threads.threads_history_post")
async def threads_history_post(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    item = str(request.query_params.get("item", "")).strip()
    if not item:
        return RedirectResponse(url="/threads/history", status_code=303)
    return FileResponse(THREADS_HISTORY_POST_PAGE)


@threads_router.get("/threads/parsers", name="threads.threads_parsers")
async def threads_parsers(request: Request):
    if not is_logged_in(request):
        return RedirectResponse(url="/login", status_code=303)
    return FileResponse(THREADS_PARSERS_PAGE)

@threads_router.get("/api/threads", name="threads.api_home")
async def api_threads_home(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    parser_status = _collect_parser_status()
    return {"ok": True, "who": username, "parser_status": parser_status}


@threads_router.get("/api/threads/accounts", name="threads.api_accounts")
async def api_threads_accounts(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    data = load_user_data(username) or {"username": username}
    ensure_threads_lists(data)
    auto_map = data.get("auto_add_latest", {}) or {}
    accounts = list(data.get("accounts", []))
    auto_out = {account: auto_map.get(normalize_account(account), True) for account in accounts}
    return {"ok": True, "who": username, "accounts": accounts, "auto_add": auto_out}


@threads_router.post("/api/threads/accounts/add", name="threads.api_add_account")
async def api_add_account(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    account = str(form.get("account", "")).strip()
    if account:
        def _mutate(data: dict):
            ensure_threads_lists(data)
            if account not in data.get("accounts", []):
                data["accounts"].append(account)

        update_user_data(username, _mutate)
    return {"ok": True}


@threads_router.post("/api/threads/accounts/remove", name="threads.api_remove_account")
async def api_remove_account(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    account = str(form.get("account", "")).strip()
    if account:
        def _mutate(data: dict):
            ensure_threads_lists(data)
            if account in data.get("accounts", []):
                data["accounts"].remove(account)

        update_user_data(username, _mutate)
    return {"ok": True}


@threads_router.post("/api/threads/accounts/auto_toggle", name="threads.api_auto_toggle")
async def api_toggle_account_auto(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    account = str(form.get("account", "")).strip()
    enabled = str(form.get("enabled", "1")).strip()
    if account:
        def _mutate(data: dict):
            ensure_threads_lists(data)
            data.setdefault("auto_add_latest", {})
            key = normalize_account(account)
            data["auto_add_latest"][key] = enabled == "1"

        update_user_data(username, _mutate)
    return {"ok": True}


@threads_router.get("/api/threads/accounts/stats", name="threads.api_accounts_stats")
async def api_threads_accounts_stats(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    item = str(request.query_params.get("item", "")).strip()
    if not item:
        return JSONResponse({"ok": False, "error": "item required"}, status_code=400)
    payload = _build_account_stats_payload(username, item)
    payload["ok"] = True
    return payload


@threads_router.get("/api/threads/posts", name="threads.api_posts")
async def api_threads_posts(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    data = load_user_data(username) or {"username": username}
    ensure_threads_lists(data)
    return {"ok": True, "who": username, "posts": list(data.get("posts", []))}


@threads_router.post("/api/threads/posts/add", name="threads.api_add_post")
async def api_add_post(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    post_item = str(form.get("post", "")).strip()
    if post_item:
        def _mutate(data: dict):
            ensure_threads_lists(data)
            if post_item not in data.get("posts", []):
                data["posts"].append(post_item)

        update_user_data(username, _mutate)
    return {"ok": True}


@threads_router.post("/api/threads/posts/remove", name="threads.api_remove_post")
async def api_remove_post(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    post_item = str(form.get("post", "")).strip()
    if post_item:
        def _mutate(data: dict):
            ensure_threads_lists(data)
            if post_item in data.get("posts", []):
                data["posts"].remove(post_item)
            stats = _ensure_stats(data).get("posts", {})
            stats.pop(post_item, None)

        update_user_data(username, _mutate)
    return {"ok": True}

@threads_router.get("/api/threads/posts/stats", name="threads.api_posts_stats")
async def api_threads_posts_stats(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    item = str(request.query_params.get("item", "")).strip()
    if not item:
        return JSONResponse({"ok": False, "error": "item required"}, status_code=400)
    payload = _build_post_stats_payload(username, item)
    payload["ok"] = True
    return payload


@threads_router.post("/api/threads/posts/refresh", name="threads.api_posts_refresh")
async def api_posts_refresh(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    item = str(form.get("item", "")).strip()
    _ = item
    return {"ok": True}


@threads_router.post("/api/threads/accounts/refresh", name="threads.api_accounts_refresh")
async def api_accounts_refresh(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    item = str(form.get("item", "")).strip()
    _ = item
    return {"ok": True}


@threads_router.get("/api/threads/history", name="threads.api_history")
async def api_threads_history(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    data = load_user_data(username) or {"username": username}
    stats = _ensure_stats(data).get("posts", {})
    items = []
    if not isinstance(stats, dict):
        stats = {}
    for url, post_stats in stats.items():
        if not isinstance(post_stats, dict):
            continue
        history_info = _post_history_payload(post_stats)
        if not history_info.get("finalized"):
            continue
        date_info = post_stats.get("dateTime", {})
        if isinstance(date_info, dict):
            post_date = date_info.get("msk_human") or date_info.get("msk") or date_info.get("raw") or ""
        else:
            post_date = str(date_info) if date_info else ""
        items.append(
            {
                "url": url,
                "started_at": history_info.get("started_at", ""),
                "started_at_human": history_info.get("started_at_human", ""),
                "completed_at": history_info.get("completed_at", ""),
                "completed_at_human": history_info.get("completed_at_human", ""),
                "post_date": post_date,
            }
        )
    items.sort(
        key=lambda x: _parse_ts(x.get("completed_at", "")) or _parse_ts(x.get("started_at", "")) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return {"ok": True, "who": username, "items": items}


@threads_router.get("/api/threads/history/post", name="threads.api_history_post")
async def api_threads_history_post(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    item = str(request.query_params.get("item", "")).strip()
    if not item:
        return JSONResponse({"ok": False, "error": "item required"}, status_code=400)
    data = load_user_data(username) or {"username": username}
    stats = _ensure_stats(data).get("posts", {})
    item_stats = stats.get(item, {}) if isinstance(stats, dict) else {}
    if not isinstance(item_stats, dict):
        item_stats = {}
    history_info = _post_history_payload(item_stats)
    if not history_info.get("finalized"):
        return JSONResponse({"ok": False, "error": "not found"}, status_code=404)
    date_info = item_stats.get("dateTime", {})
    if isinstance(date_info, dict):
        post_date = date_info.get("msk_human") or date_info.get("msk") or date_info.get("raw") or ""
    else:
        post_date = str(date_info) if date_info else ""
    return {
        "ok": True,
        "who": username,
        "item": item,
        "post_date": post_date,
        "metric_options": history_info.get("metric_options", []),
        "hourly_by_metric": history_info.get("hourly_by_metric", {}),
        "ready_hours": history_info.get("ready_hours", 0),
        "started_at_human": history_info.get("started_at_human", ""),
        "completed_at_human": history_info.get("completed_at_human", ""),
    }


@threads_router.post("/api/threads/history/delete", name="threads.api_history_delete")
async def api_threads_history_delete(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    item = str(form.get("item", "")).strip()
    if not item:
        return {"ok": True}
    def _mutate(data: dict):
        ensure_threads_lists(data)
        if item in data.get("posts", []):
            data["posts"].remove(item)
        if isinstance(data.get("seen_posts"), list):
            try:
                data["seen_posts"].remove(item)
            except ValueError:
                pass
        stats = _ensure_stats(data)
        stats.get("posts", {}).pop(item, None)
        for acc_val in stats.get("accounts", {}).values():
            if not isinstance(acc_val, dict):
                continue
            post_history = acc_val.get("post_history", {})
            if isinstance(post_history, dict) and item in post_history:
                post_history.pop(item, None)
                acc_val["post_history"] = post_history
            pending = acc_val.get("pending_post_history", [])
            if isinstance(pending, list):
                pending = [p for p in pending if not (isinstance(p, dict) and p.get("post_url") == item)]
                acc_val["pending_post_history"] = pending
            tracking = acc_val.get("last_post_tracking", {})
            if isinstance(tracking, dict) and tracking.get("post_url") == item:
                acc_val["last_post_tracking"] = {}

    update_user_data(username, _mutate)
    return {"ok": True}


@threads_router.get("/api/threads/parsers", name="threads.api_parsers")
async def api_threads_parsers(request: Request):
    username, error = _require_user(request)
    if error:
        return error
    parser_status = _collect_parser_status()
    status_posts = posts_parser_running()
    status_accounts = accounts_parser_running()
    return {
        "ok": True,
        "who": username,
        "status_posts": status_posts,
        "status_accounts": status_accounts,
        "parser_status": parser_status,
    }


@threads_router.post("/api/threads/parsers/control", name="threads.api_parsers_control")
async def api_threads_parsers_control(request: Request):
    _, error = _require_user(request)
    if error:
        return error
    form = await request.form()
    action = str(form.get("action", "")).strip()
    control_parsers_action(action)
    return {"ok": True}
