#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import time
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import unquote
from urllib.request import Request, urlopen

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
HEALTH_PATH = RUNTIME_DIR / "parser_health_posts.json"
BATCH_SIZE = 10        # размер батча постов
PER_LINK_DELAY = 1.5   # задержка между запросами к постам, секунд
BATCH_DELAY = 5        # задержка между батчами, секунд
CYCLE_DELAY = 30       # период цикла опроса, секунд
TRACK_METRICS = ("views", "likes", "comments", "repost", "shared")
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ACCEPT_LANGUAGE = "en-US,en;q=0.9"
MSK_TZ = timezone(timedelta(hours=3))
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


def _send_stats_to_backend(stats_by_url: dict) -> bool:
    if not isinstance(stats_by_url, dict) or not stats_by_url:
        return True

    payload = {
        "type": "posts_stats_batch",
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
                print(f"[posts-parser] backend status: {status_code}")
                return False
        return True
    except HTTPError as exc:
        print(f"[posts-parser] backend HTTP error: {exc.code}")
        return False
    except URLError as exc:
        print(f"[posts-parser] backend URL error: {exc.reason}")
        return False
    except Exception as exc:
        print(f"[posts-parser] backend push failed: {exc}")
        return False

def _first_non_empty(*values: Optional[str]) -> str:
    for value in values:
        if value:
            stripped = value.strip()
            if stripped:
                return stripped
    return ""


class _PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._title_active = False
        self._article_depth = 0
        self._main_depth = 0
        self.title_parts: list[str] = []
        self.article_parts: list[str] = []
        self.main_parts: list[str] = []
        self.canonical = ""
        self.meta: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs) -> None:
        attrs_map = {str(k).lower(): (v or "") for k, v in attrs}
        tag_lower = tag.lower()

        if tag_lower == "title":
            self._title_active = True
            return
        if tag_lower == "article":
            self._article_depth += 1
            return
        if tag_lower == "main":
            self._main_depth += 1
            return
        if tag_lower == "link":
            rel = attrs_map.get("rel", "").lower()
            if "canonical" in rel and not self.canonical:
                href = attrs_map.get("href", "").strip()
                if href:
                    self.canonical = href
            return
        if tag_lower != "meta":
            return

        key = (attrs_map.get("property") or attrs_map.get("name") or "").strip()
        content = (attrs_map.get("content") or "").strip()
        if key and content and key not in self.meta:
            self.meta[key] = content

    def handle_endtag(self, tag: str) -> None:
        tag_lower = tag.lower()
        if tag_lower == "title":
            self._title_active = False
        elif tag_lower == "article":
            self._article_depth = max(0, self._article_depth - 1)
        elif tag_lower == "main":
            self._main_depth = max(0, self._main_depth - 1)

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if not text:
            return
        if self._title_active:
            self.title_parts.append(text)
        if self._article_depth > 0:
            self.article_parts.append(text)
        if self._main_depth > 0:
            self.main_parts.append(text)


def _extract_page_data(html: str) -> dict:
    parser = _PageParser()
    try:
        parser.feed(html or "")
        parser.close()
    except Exception:
        pass

    title = " ".join(parser.title_parts).strip()
    text_candidates = {
        "article": " ".join(parser.article_parts).strip(),
        "main": " ".join(parser.main_parts).strip(),
    }
    meta_fields = [
        "og:title",
        "og:description",
        "og:image",
        "og:url",
        "twitter:title",
        "twitter:description",
        "twitter:image",
        "description",
    ]
    meta = {field: parser.meta.get(field, "") for field in meta_fields}

    return {
        "title": title,
        "canonical": parser.canonical,
        "meta": {k: v for k, v in meta.items() if v},
        "text_candidates": {k: v for k, v in text_candidates.items() if v},
    }


def scrape_post(
    url: str,
    headful: bool,
    timeout_ms: int,
    wait_ms: int,
    session: Optional[Session] = None,
) -> Dict[str, object]:
    _ = headful  # CLI compatibility flag; not used by curl mode.
    owns_session = session is None
    if owns_session:
        session = Session(impersonate="chrome142")

    response_status = None
    response_ok = False
    response_headers: dict = {}
    response_text = ""

    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": ACCEPT_LANGUAGE,
    }
    timeout_sec = max(5.0, float(timeout_ms) / 1000.0)

    try:
        _health_update(
            last_request_ts_utc=datetime.now(timezone.utc).isoformat(),
            last_request_url=url,
        )
        response = session.get(
            url,
            headers=headers,
            timeout=timeout_sec,
        )
        _health_update(
            last_response_ts_utc=datetime.now(timezone.utc).isoformat(),
            last_response_url=url,
            last_response_status=getattr(response, "status_code", None),
        )
        if wait_ms:
            time.sleep(max(0, wait_ms) / 1000.0)
        response_status = getattr(response, "status_code", None)
        response_ok = bool(getattr(response, "ok", False))
        try:
            response_headers = dict(getattr(response, "headers", {}) or {})
        except Exception:
            response_headers = {}
        response_text = getattr(response, "text", "") or ""
        if not response_text:
            content = getattr(response, "content", b"") or b""
            if isinstance(content, bytes):
                response_text = content.decode("utf-8", errors="ignore")
            else:
                response_text = str(content)
    except Exception:
        _health_update(
            last_error_ts_utc=datetime.now(timezone.utc).isoformat(),
            last_error=f"request_failed:{url}",
        )
        response_text = ""
    finally:
        if owns_session and session is not None:
            try:
                session.close()
            except Exception:
                pass

    page_data = _extract_page_data(response_text)
    meta_data = page_data.get("meta") if isinstance(page_data.get("meta"), dict) else {}
    text_data = (
        page_data.get("text_candidates")
        if isinstance(page_data.get("text_candidates"), dict)
        else {}
    )
    best_text = _first_non_empty(
        meta_data.get("og:description"),
        meta_data.get("twitter:description"),
        meta_data.get("description"),
        text_data.get("article"),
        text_data.get("main"),
    )

    fetched_at = datetime.now(timezone.utc).isoformat()
    content_type = ""
    for key, value in response_headers.items():
        if str(key).lower() == "content-type":
            content_type = str(value)
            break

    return {
        "url": url,
        "canonical": page_data.get("canonical", ""),
        "title": page_data.get("title", ""),
        "response": {
            "status": response_status,
            "ok": response_ok,
            "headers": response_headers,
            "content_type": content_type,
            "text": response_text,
        },
        "meta": meta_data,
        "text": best_text,
        "text_candidates": text_data,
        "fetched_at_utc": fetched_at,
    }


def extract_counts(text: str) -> Dict[str, Optional[int]]:
    keys = [
        "view_counts",
        "direct_reply_count",
        "repost_count",
        "quote_count",
        "reshare_count",
        "like_count",
    ]
    results: Dict[str, Optional[int]] = {k: None for k in keys}
    if not text:
        return results
    pattern = re.compile(
        r'"(?P<key>view_counts|direct_reply_count|repost_count|quote_count|reshare_count|like_count)"\s*:\s*(?P<val>null|\d+)'
    )
    remaining = set(keys)
    for match in pattern.finditer(text):
        key = match.group("key")
        if key in remaining:
            raw_val = match.group("val")
            results[key] = 0 if raw_val == "null" else int(raw_val)
            remaining.remove(key)
            if not remaining:
                break
    return results


def extract_balanced_block(text: str, start: int):
    if start >= len(text) or text[start] not in "{[":
        return None, start

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        ch = text[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch in "{[":
            depth += 1
        elif ch in "]}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1], i + 1

    return None, start


def extract_node_blocks(text: str):
    nodes = []
    needle = '"node":'
    idx = 0

    while True:
        idx = text.find(needle, idx)
        if idx == -1:
            break

        j = idx + len(needle)
        while j < len(text) and text[j].isspace():
            j += 1

        if j >= len(text) or text[j] not in "{[":
            idx = j
            continue

        block, end = extract_balanced_block(text, j)
        if block:
            nodes.append(block)
            idx = end
        else:
            idx = j + 1

    return nodes


def extract_pairs_from_node(text: str):
    # Pair each username with the nearest following plaintext.
    pattern = re.compile(r'"(username|plaintext)"\s*:\s*"((?:\\.|[^"\\])*)"')
    pairs = []
    current_username = None

    for match in pattern.finditer(text):
        key = match.group(1)
        raw_value = match.group(2)
        try:
            value = json.loads(f'"{raw_value}"')
        except Exception:
            value = raw_value

        if key == "username":
            current_username = value
        else:
            if current_username is not None:
                pairs.append((current_username, value))
                current_username = None

    return pairs


def extract_datetime(text: str) -> Dict[str, str]:
    if not text:
        return {"raw": ""}
    match = re.search(r'dateTime="([^"]+)"', text)
    if not match:
        match = re.search(r"dateTime=([^&\"\\s]+)", text)
    if not match:
        match = re.search(r'"dateTime"\s*:\s*"([^"]+)"', text)
    if not match:
        return {"raw": ""}
    raw = unquote(match.group(1))
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except Exception:
        return {"raw": raw}

    msk_tz = timezone(timedelta(hours=3))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=msk_tz)
    msk = parsed.astimezone(msk_tz)
    return {
        "raw": raw,
        "msk": msk.isoformat(),
        "msk_human": msk.strftime("%Y-%m-%d %H:%M:%S MSK"),
    }


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


def _ensure_stats(data: dict) -> dict:
    stats = data.setdefault("stats", {})
    stats.setdefault("accounts", {})
    stats.setdefault("posts", {})
    return stats


def _collect_posts(users: list[tuple[Path, dict]]) -> list[str]:
    posts = []
    seen = set()
    for _, data in users:
        for item in data.get("posts", []) if isinstance(data, dict) else []:
            value = str(item).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            posts.append(value)
    return posts


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


def _format_hour_range(start_ts: datetime, end_ts: datetime) -> str:
    start_msk = start_ts.astimezone(MSK_TZ)
    end_msk = end_ts.astimezone(MSK_TZ)
    return f"{start_msk:%H:%M}–{end_msk:%H:%M}"


def _metric_value(point: dict, metric: str):
    if not isinstance(point, dict):
        return None
    value = point.get(metric)
    return value if isinstance(value, int) else None


def _value_at_time(timeline: list[dict], target_ts: datetime, metric: str):
    value = None
    for point in timeline:
        ts = _parse_ts(str(point.get("ts_utc", "")))
        if not ts or ts > target_ts:
            break
        current = _metric_value(point, metric)
        if current is not None:
            value = current
    return value


def _compute_hourly_metric(timeline: list[dict], start_ts: datetime, metric: str) -> list[dict]:
    base = _value_at_time(timeline, start_ts, metric)
    if base is None:
        base = 0
    prev_value = base
    rows = []
    for hour in range(1, 25):
        end_ts = start_ts + timedelta(hours=hour)
        value = _value_at_time(timeline, end_ts, metric)
        if value is None:
            value = prev_value
        rows.append(
            {
                "hour": hour,
                "range": _format_hour_range(end_ts - timedelta(hours=1), end_ts),
                "delta": value - prev_value,
            }
        )
        prev_value = value
    return rows


def _normalize_timeline(raw_timeline) -> list[dict]:
    if not isinstance(raw_timeline, list):
        return []
    bucket = {}
    for entry in raw_timeline:
        if not isinstance(entry, dict):
            continue
        ts_utc = str(entry.get("ts_utc", "")).strip()
        ts = _parse_ts(ts_utc)
        if not ts:
            continue
        point = {"ts_utc": ts.isoformat()}
        for metric in TRACK_METRICS:
            value = entry.get(metric)
            point[metric] = value if isinstance(value, int) else None
        bucket[point["ts_utc"]] = point
    ordered = sorted(bucket.values(), key=lambda p: _parse_ts(p["ts_utc"]) or datetime.min.replace(tzinfo=timezone.utc))
    return ordered


def _normalize_tracking(raw_tracking) -> dict:
    tracking = raw_tracking if isinstance(raw_tracking, dict) else {}
    out = {}
    started_dt = _parse_ts(str(tracking.get("started_at_utc", "")))
    if started_dt:
        out["started_at_utc"] = started_dt.isoformat()
    timeline = _normalize_timeline(tracking.get("timeline"))
    if timeline:
        out["timeline"] = timeline
    history = tracking.get("history_24h")
    if isinstance(history, dict):
        out["history_24h"] = json.loads(json.dumps(history))
    return out


def _tracking_rank(tracking: dict):
    started = _parse_ts(str(tracking.get("started_at_utc", "")))
    start_rank = started.timestamp() if started else float("inf")
    timeline = tracking.get("timeline", [])
    timeline_len = len(timeline) if isinstance(timeline, list) else 0
    finalized = 1 if isinstance(tracking.get("history_24h"), dict) and tracking.get("history_24h", {}).get("finalized") else 0
    return (start_rank, -finalized, -timeline_len)


def _merge_tracking(base_tracking, extra_tracking) -> dict:
    base = _normalize_tracking(base_tracking)
    extra = _normalize_tracking(extra_tracking)

    started_candidates = []
    for candidate in (
        str(base.get("started_at_utc", "")),
        str(extra.get("started_at_utc", "")),
    ):
        ts = _parse_ts(candidate)
        if ts:
            started_candidates.append(ts)
    started_dt = min(started_candidates) if started_candidates else None

    timeline = _normalize_timeline(
        (base.get("timeline") or []) + (extra.get("timeline") or [])
    )

    out = {}
    if started_dt:
        out["started_at_utc"] = started_dt.isoformat()
    if timeline:
        out["timeline"] = timeline

    base_hist = base.get("history_24h") if isinstance(base.get("history_24h"), dict) else {}
    extra_hist = extra.get("history_24h") if isinstance(extra.get("history_24h"), dict) else {}
    if _tracking_rank({"history_24h": extra_hist, "timeline": extra.get("timeline", [])}) < _tracking_rank({"history_24h": base_hist, "timeline": base.get("timeline", [])}):
        best_hist = extra_hist
    else:
        best_hist = base_hist
    if best_hist:
        out["history_24h"] = best_hist
    return out


def _collect_global_post_tracking(users: list[tuple[Path, dict]]) -> dict:
    global_map = {}
    for _, data in users:
        stats = data.get("stats", {}) if isinstance(data, dict) else {}
        posts_stats = stats.get("posts", {}) if isinstance(stats, dict) else {}
        if not isinstance(posts_stats, dict):
            continue
        for url, snapshot in posts_stats.items():
            if not isinstance(snapshot, dict):
                continue
            tracking = _normalize_tracking(snapshot.get("tracking"))
            if not tracking:
                continue
            prev = global_map.get(url)
            merged = _merge_tracking(prev, tracking)
            global_map[url] = merged
    return global_map


def _apply_tracking_horizon(tracking: dict) -> dict:
    started_dt = _parse_ts(str(tracking.get("started_at_utc", "")))
    timeline = _normalize_timeline(tracking.get("timeline"))
    if not started_dt:
        tracking["timeline"] = timeline
        return tracking
    horizon = started_dt + timedelta(hours=24)
    tracking["timeline"] = [
        point for point in timeline
        if (lambda ts: ts is not None and ts <= horizon)(_parse_ts(str(point.get("ts_utc", ""))))
    ]
    return tracking


def _update_history_24h(tracking: dict, now_ts: datetime):
    started_dt = _parse_ts(str(tracking.get("started_at_utc", "")))
    timeline = tracking.get("timeline", [])
    if not started_dt or not isinstance(timeline, list):
        return tracking

    ready_hours = int((now_ts - started_dt).total_seconds() // 3600)
    if ready_hours < 0:
        ready_hours = 0
    if ready_hours > 24:
        ready_hours = 24

    metrics = {}
    for metric in TRACK_METRICS:
        all_rows = _compute_hourly_metric(timeline, started_dt, metric)
        metrics[metric] = all_rows[:ready_hours]

    history = {
        "start_ts_utc": started_dt.isoformat(),
        "ready_hours": ready_hours,
        "finalized": ready_hours >= 24,
        "updated_at_utc": now_ts.isoformat(),
        "metrics": metrics,
    }
    if ready_hours >= 24:
        history["completed_at_utc"] = (started_dt + timedelta(hours=24)).isoformat()

    tracking["history_24h"] = history
    return tracking


def _update_users_posts_stats(users: list[tuple[Path, dict]], stats_by_url: dict) -> None:
    global_tracking = _collect_global_post_tracking(users)
    for path, data in users:
        with path_lock(path):
            current = data
            try:
                fresh = json.loads(path.read_text(encoding="utf-8-sig"))
                if isinstance(fresh, dict):
                    current = fresh
            except Exception:
                pass
            stats = _ensure_stats(current)
            posts_stats = stats.get("posts", {})
            for item in current.get("posts", []) if isinstance(current, dict) else []:
                key = str(item).strip()
                if key in stats_by_url:
                    snapshot = dict(stats_by_url[key])
                    existing = posts_stats.get(key, {}) if isinstance(posts_stats.get(key), dict) else {}
                    existing_tracking = existing.get("tracking", {}) if isinstance(existing, dict) else {}
                    tracking = _merge_tracking(existing_tracking, global_tracking.get(key, {}))
                    now_iso = datetime.now(timezone.utc).isoformat()
                    started_iso = tracking.get("started_at_utc") or snapshot.get("fetched_at_utc") or now_iso
                    started_dt = _parse_ts(started_iso)
                    if not started_dt:
                        started_dt = datetime.now(timezone.utc)
                        started_iso = started_dt.isoformat()
                    tracking["started_at_utc"] = started_iso
                    timeline = _normalize_timeline(tracking.get("timeline", []))
                    point = {
                        "ts_utc": snapshot.get("fetched_at_utc") or now_iso,
                        "views": snapshot.get("views"),
                        "likes": snapshot.get("likes"),
                        "comments": snapshot.get("comments"),
                        "repost": snapshot.get("repost"),
                        "shared": snapshot.get("shared"),
                    }
                    timeline = _normalize_timeline(timeline + [point])
                    tracking["timeline"] = timeline
                    tracking = _apply_tracking_horizon(tracking)
                    tracking = _update_history_24h(tracking, datetime.now(timezone.utc))
                    snapshot["tracking"] = tracking
                    posts_stats[key] = snapshot
                    global_tracking[key] = _merge_tracking(global_tracking.get(key), tracking)
            _save_user(path, current)


def process_posts(
    posts: list[str],
    headful: bool,
    timeout_ms: int,
    wait_ms: int,
    delay_ms: int,
) -> dict:
    stats_by_url = {}
    with Session(impersonate="chrome142") as session:
        for index, url in enumerate(posts, start=1):
            data = scrape_post(
                url=url,
                headful=headful,
                timeout_ms=timeout_ms,
                wait_ms=wait_ms,
                session=session,
            )

            response_text = ""
            response = data.get("response", {}) or {}
            if isinstance(response, dict):
                response_text = response.get("text") or ""

            counts = extract_counts(response_text)
            data["counts"] = counts
            date_info = extract_datetime(response_text)
            data["dateTime"] = date_info

            print(f"[{index}/{len(posts)}] {url}")
            for key, value in counts.items():
                print(f"{key}: {value}")
            print(
                f'dateTime: {date_info.get("msk_human") or date_info.get("msk") or date_info.get("raw") or "not_found"}'
            )

            nodes = extract_node_blocks(response_text)
            comments = []
            comments_threads = []
            for node_str in nodes[1:]:
                thread = extract_pairs_from_node(node_str)
                if thread:
                    comments_threads.append(thread)
                    comments.extend(thread)

            if comments:
                print(f"Comments: {url}")
                for username, plaintext in comments:
                    print(f"username: {username}")
                    print(f"plaintext: {plaintext}")
                    print("-" * 20)
                print("-" * 40)

            if delay_ms and index < len(posts):
                time.sleep(delay_ms / 1000.0)

            stats_by_url[url] = {
                "views": counts.get("view_counts"),
                "likes": counts.get("like_count"),
                "comments": counts.get("direct_reply_count"),
                "repost": counts.get("repost_count"),
                "shared": counts.get("reshare_count"),
                "dateTime": date_info,
                "fetched_at_utc": data.get("fetched_at_utc"),
                "comments_list": [{"username": u, "text": t} for u, t in comments] if comments else [],
                "comments_threads": [
                    [{"username": u, "text": t} for u, t in thread] for thread in comments_threads
                ] if comments_threads else [],
            }

    return stats_by_url


def poll_posts(
    headful: bool,
    timeout_ms: int,
    wait_ms: int,
    delay_ms: int,
    interval_sec: int,
) -> None:
    while True:
        try:
            users = _load_users()
            posts = _collect_posts(users)
            if posts:
                stats_by_url = {}
                batches = [posts[i : i + BATCH_SIZE] for i in range(0, len(posts), BATCH_SIZE)]
                for idx, batch in enumerate(batches, start=1):
                    batch_stats = process_posts(
                        posts=batch,
                        headful=headful,
                        timeout_ms=timeout_ms,
                        wait_ms=wait_ms,
                        delay_ms=delay_ms,
                    )
                    stats_by_url.update(batch_stats)
                    if idx < len(batches):
                        time.sleep(BATCH_DELAY)
                _send_stats_to_backend(stats_by_url)
        except Exception as exc:  # pragma: no cover - runtime resilience
            print(f"[poll_posts] error: {exc}")
        time.sleep(max(5, interval_sec))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="curl_cffi post parser (per-user JSON)."
    )
    parser.add_argument("--list", action="store_true", help="List tracked post URLs")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run a single parsing cycle and exit",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="Compatibility flag (ignored in curl mode)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=45000,
        help="Request timeout in milliseconds",
    )
    parser.add_argument(
        "--wait-ms",
        type=int,
        default=2000,
        help="Extra wait after request in milliseconds",
    )
    parser.add_argument(
        "--delay-ms",
        type=int,
        default=750,
        help="Delay between post requests (milliseconds)",
    )
    parser.add_argument(
        "--interval-sec",
        type=int,
        default=CYCLE_DELAY,
        help="Polling interval in seconds",
    )

    args = parser.parse_args()
    _health_update(
        parser="posts",
        pid=os.getpid(),
        started_at_utc=datetime.now(timezone.utc).isoformat(),
    )

    if args.list:
        users = _load_users()
        posts = _collect_posts(users)
        for url in posts:
            print(url)
        return 0

    if args.test:
        users = _load_users()
        posts = _collect_posts(users)
        if posts:
            stats_by_url = process_posts(
                posts=posts,
                headful=args.headful,
                timeout_ms=args.timeout_ms,
                wait_ms=args.wait_ms,
                delay_ms=args.delay_ms,
            )
            _send_stats_to_backend(stats_by_url)
        return 0

    poll_posts(
        headful=args.headful,
        timeout_ms=args.timeout_ms,
        wait_ms=args.wait_ms,
        delay_ms=args.delay_ms,
        interval_sec=args.interval_sec,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
