#!/usr/bin/env python3
"""Sync curated and daily history from aihot.virxact.com."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import parse as urllib_parse
from urllib import request as urllib_request

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from runtime_config import RuntimeConfigStore

ROOT = SCRIPT_DIR.parent
DEFAULT_BASE_URL = "https://aihot.virxact.com"
DEFAULT_SOURCE_LABEL = "卡兹克"
DEFAULT_CATEGORY = "卡兹克"
DEFAULT_SELECTED_CATEGORY = "卡兹克精选"
DEFAULT_DAILY_CATEGORY = "卡兹克日报"
DEFAULT_CATEGORY_EMOJI = "🦂"
DEFAULT_PAGE_DELAY_MS = 900
DEFAULT_SELECTED_SOURCE_ID = "aihot-selected"
DEFAULT_DAILY_SOURCE_ID = "aihot-daily"
DEFAULT_SELECTED_TAKE = 100
DEFAULT_DAILIES_TAKE = 100
DISPLAY_TZ = timezone(timedelta(hours=8))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"


def _request_json(url, *, timeout=30):
    req = urllib_request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "application/json",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        },
    )
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _merge_rows(existing, fresh, key):
    merged = {row.get(key): dict(row) for row in existing if row.get(key)}
    for row in fresh:
        merged[row.get(key)] = dict(row)
    rows = list(merged.values())
    rows.sort(key=lambda row: (row.get("published_at") or "", row.get(key) or ""), reverse=True)
    return rows


def _save_source_day(repo_dir, source, date_str, items, *, card_title, overview):
    source_id = source["id"]
    card_id = f"aihot::{source_id}::{date_str}"
    item_rows = []
    for item in items:
        row = dict(item)
        row["date"] = date_str
        row["card_msg_id"] = card_id
        item_rows.append(row)
    card_rows = [{
        "message_id": card_id,
        "title": card_title,
        "source": (source.get("config") or {}).get("source_label") or DEFAULT_SOURCE_LABEL,
        "count": len(item_rows),
        "segment": "AIHOT",
        "items_count": len(item_rows),
        "overview": overview,
        "date": date_str,
    }]

    cards_path = Path(repo_dir) / "data" / source_id / "cards" / f"{date_str}.jsonl"
    items_path = Path(repo_dir) / "data" / source_id / "items" / f"{date_str}.jsonl"
    _write_jsonl(cards_path, _merge_rows(_read_jsonl(cards_path), card_rows, "message_id"))
    _write_jsonl(items_path, _merge_rows(_read_jsonl(items_path), item_rows, "item_id"))


def _rebuild_web_day(repo_dir, date_str):
    subprocess.run([sys.executable, str(Path(repo_dir) / "scripts" / "build-web-data.py"), "--date", date_str], check=True)


def _to_iso(dt):
    if isinstance(dt, str):
        return dt
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _display_date(value):
    dt = _parse_iso_datetime(value)
    if not dt:
        return datetime.now(DISPLAY_TZ).strftime("%Y-%m-%d")
    return dt.astimezone(DISPLAY_TZ).strftime("%Y-%m-%d")


def _latest_imported_published_at(repo_dir, source_id):
    items_dir = Path(repo_dir) / "data" / source_id / "items"
    if not items_dir.exists():
        return None
    latest_dt = None
    for path in sorted(items_dir.glob("*.jsonl"), reverse=True)[:5]:
        for row in _read_jsonl(path):
            dt = _parse_iso_datetime(row.get("published_at"))
            if dt and (latest_dt is None or dt > latest_dt):
                latest_dt = dt
    return latest_dt


def _existing_daily_dates(repo_dir, source_id):
    items_dir = Path(repo_dir) / "data" / source_id / "items"
    if not items_dir.exists():
        return set()
    return {path.stem for path in items_dir.glob("*.jsonl")}


def _sleep_ms(delay_ms):
    if delay_ms and delay_ms > 0:
        time.sleep(delay_ms / 1000.0)


def _selected_item_row(source, item):
    config = source.get("config") or {}
    source_id = source["id"]
    published_at = item.get("publishedAt") or datetime.now(timezone.utc).isoformat()
    origin_category = item.get("category") or ""
    stable_key = item.get("id") or hashlib.sha1(f"{item.get('url')}|{item.get('title')}".encode("utf-8")).hexdigest()[:16]
    return {
        "item_id": f"{source_id}::{stable_key}",
        "category": config.get("category") or DEFAULT_CATEGORY,
        "raw_cat": origin_category or config.get("category") or DEFAULT_CATEGORY,
        "title": item.get("title") or "(无标题)",
        "summary": item.get("summary") or "",
        "author": item.get("source") or "",
        "url": item.get("url") or "",
        "source": config.get("source_label") or DEFAULT_SOURCE_LABEL,
        "segment": "AIHOT",
        "published_at": published_at,
        "origin_source": item.get("source") or "",
        "origin_category": origin_category,
        "origin_title_en": item.get("title_en"),
        "origin_mode": "selected",
        "origin_item_id": item.get("id"),
    }


def _daily_item_row(source, report_date, section_label, item, *, generated_at, kind="section"):
    config = source.get("config") or {}
    source_id = source["id"]
    origin_url = item.get("sourceUrl") or item.get("url") or ""
    stable_material = "|".join([report_date, section_label, item.get("title") or "", origin_url, kind])
    stable_key = hashlib.sha1(stable_material.encode("utf-8")).hexdigest()[:16]
    return {
        "item_id": f"{source_id}::{stable_key}",
        "category": config.get("category") or DEFAULT_CATEGORY,
        "raw_cat": section_label or config.get("category") or DEFAULT_CATEGORY,
        "title": item.get("title") or "(无标题)",
        "summary": item.get("summary") or "",
        "author": item.get("sourceName") or "",
        "url": origin_url,
        "source": config.get("source_label") or DEFAULT_SOURCE_LABEL,
        "segment": "AIHOT",
        "published_at": generated_at,
        "origin_source": item.get("sourceName") or "",
        "origin_category": section_label,
        "origin_mode": "daily",
        "origin_kind": kind,
    }


def _fetch_selected_page(base_url, *, take, cursor=None, since=None):
    params = {"mode": "selected", "take": str(int(take or DEFAULT_SELECTED_TAKE))}
    if cursor:
        params["cursor"] = cursor
    if since:
        params["since"] = since
    url = f"{base_url.rstrip('/')}/api/public/items?{urllib_parse.urlencode(params)}"
    return _request_json(url)


def _fetch_dailies_index(base_url, *, take):
    url = f"{base_url.rstrip('/')}/api/public/dailies?{urllib_parse.urlencode({'take': str(int(take or DEFAULT_DAILIES_TAKE))})}"
    return _request_json(url)


def _fetch_daily_report(base_url, date_str):
    return _request_json(f"{base_url.rstrip('/')}/api/public/daily/{date_str}")


def _sync_selected_source(repo_dir, source, *, history=False, page_delay_ms=DEFAULT_PAGE_DELAY_MS, with_web=False):
    config = source.get("config") or {}
    base_url = config.get("base_url") or DEFAULT_BASE_URL
    take = int(config.get("take") or DEFAULT_SELECTED_TAKE)
    since = None
    if not history:
        latest_dt = _latest_imported_published_at(repo_dir, source["id"])
        if latest_dt:
            since = _to_iso(latest_dt - timedelta(days=2))

    cursor = None
    affected_dates = set()
    page_count = 0
    imported_count = 0
    while True:
        payload = _fetch_selected_page(base_url, take=take, cursor=cursor, since=since)
        items = payload.get("items") or []
        if not items:
            break
        by_date = defaultdict(list)
        for item in items:
            row = _selected_item_row(source, item)
            date_str = _display_date(row["published_at"])
            by_date[date_str].append(row)
            imported_count += 1
        for date_str, day_items in by_date.items():
            _save_source_day(
                repo_dir,
                source,
                date_str,
                day_items,
                card_title=f"{source.get('name') or DEFAULT_SOURCE_LABEL} · {date_str}",
                overview=f"{source.get('name') or DEFAULT_SOURCE_LABEL}精选：{len(day_items)} 条",
            )
            affected_dates.add(date_str)
            if with_web:
                _rebuild_web_day(repo_dir, date_str)
        page_count += 1
        cursor = payload.get("nextCursor")
        if not payload.get("hasNext") or not cursor:
            break
        _sleep_ms(page_delay_ms)

    return {
        "source_id": source["id"],
        "pages": page_count,
        "items": imported_count,
        "dates": sorted(affected_dates),
    }


def _sync_daily_source(repo_dir, source, *, history=False, page_delay_ms=DEFAULT_PAGE_DELAY_MS, with_web=False):
    config = source.get("config") or {}
    base_url = config.get("base_url") or DEFAULT_BASE_URL
    take = int(config.get("daily_take") or config.get("take") or DEFAULT_DAILIES_TAKE)
    payload = _fetch_dailies_index(base_url, take=take)
    daily_rows = payload.get("items") or []
    existing_dates = _existing_daily_dates(repo_dir, source["id"])
    wanted_dates = []
    for row in daily_rows:
        date_str = str(row.get("date") or "").strip()
        if not date_str:
            continue
        if history or date_str not in existing_dates or date_str >= (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d"):
            wanted_dates.append(date_str)

    affected_dates = set()
    imported_count = 0
    for idx, date_str in enumerate(sorted(set(wanted_dates))):
        report = _fetch_daily_report(base_url, date_str)
        generated_at = report.get("generatedAt") or f"{date_str}T00:00:00Z"
        rows = []
        for section in report.get("sections") or []:
            label = section.get("label") or "日报"
            for item in section.get("items") or []:
                rows.append(_daily_item_row(source, date_str, label, item, generated_at=generated_at, kind="section"))
        for flash in report.get("flashes") or []:
            rows.append(_daily_item_row(source, date_str, "快讯", flash, generated_at=generated_at, kind="flash"))
        if rows:
            _save_source_day(
                repo_dir,
                source,
                date_str,
                rows,
                card_title=f"{source.get('name') or DEFAULT_SOURCE_LABEL} · {date_str}",
                overview=f"{source.get('name') or DEFAULT_SOURCE_LABEL}日报：{len(rows)} 条",
            )
            affected_dates.add(date_str)
            imported_count += len(rows)
            if with_web:
                _rebuild_web_day(repo_dir, date_str)
        if idx < len(set(wanted_dates)) - 1:
            _sleep_ms(page_delay_ms)

    return {
        "source_id": source["id"],
        "reports": len(set(wanted_dates)),
        "items": imported_count,
        "dates": sorted(affected_dates),
    }


def ensure_default_aihot_sources(repo_dir=ROOT):
    repo_dir = Path(repo_dir)
    store = RuntimeConfigStore(repo_dir / "config.yaml", repo_dir / "config.local.json")
    store.upsert_category({"label": DEFAULT_SELECTED_CATEGORY, "emoji": DEFAULT_CATEGORY_EMOJI})
    store.upsert_category({"label": DEFAULT_DAILY_CATEGORY, "emoji": DEFAULT_CATEGORY_EMOJI})
    try:
        store.delete_category(DEFAULT_CATEGORY)
    except Exception:
        pass
    merged = store.load()
    sources = deepcopy(merged.get("sources", []) or [])
    defaults = [
        {
            "id": DEFAULT_SELECTED_SOURCE_ID,
            "name": "卡兹克精选",
            "type": "aihot_api",
            "enabled": True,
            "config": {
                "base_url": DEFAULT_BASE_URL,
                "mode": "selected",
                "take": DEFAULT_SELECTED_TAKE,
                "category": DEFAULT_SELECTED_CATEGORY,
                "source_label": DEFAULT_SOURCE_LABEL,
            },
            "derived": ["web"],
        },
        {
            "id": DEFAULT_DAILY_SOURCE_ID,
            "name": "卡兹克日报",
            "type": "aihot_api",
            "enabled": True,
            "config": {
                "base_url": DEFAULT_BASE_URL,
                "mode": "daily",
                "daily_take": DEFAULT_DAILIES_TAKE,
                "category": DEFAULT_DAILY_CATEGORY,
                "source_label": DEFAULT_SOURCE_LABEL,
            },
            "derived": ["web"],
        },
    ]
    for incoming in defaults:
        existing_idx = next((idx for idx, item in enumerate(sources) if item.get("id") == incoming["id"]), None)
        if existing_idx is None:
            sources.append(incoming)
        else:
            current = sources[existing_idx]
            sources[existing_idx] = {
                **current,
                **incoming,
                "config": {**(current.get("config") or {}), **incoming["config"]},
            }
    saved = store._save_top_level_key("sources", sources)
    return {"ok": True, "sources": saved.get("sources", []), "categories": saved.get("categories", [])}


def sync_aihot_sources_report(
    repo_dir=ROOT,
    source_ids=None,
    with_web=False,
    history=False,
    page_delay_ms=DEFAULT_PAGE_DELAY_MS,
):
    repo_dir = Path(repo_dir)
    cfg = RuntimeConfigStore(repo_dir / "config.yaml", repo_dir / "config.local.json").load()
    source_filter = {str(source_id).strip() for source_id in (source_ids or []) if str(source_id).strip()}
    enabled_sources = []
    for source in cfg.get("sources", []) or []:
        if source.get("type") != "aihot_api" or not source.get("enabled", True):
            continue
        if source_filter and source.get("id") not in source_filter:
            continue
        enabled_sources.append(source)

    attempted = []
    success = []
    failed = []
    affected_dates = set()
    details = []
    for idx, source in enumerate(enabled_sources):
        attempted.append(source["id"])
        mode = str((source.get("config") or {}).get("mode") or "selected").strip().lower()
        try:
            if mode == "daily":
                detail = _sync_daily_source(repo_dir, source, history=history, page_delay_ms=page_delay_ms, with_web=with_web)
            else:
                detail = _sync_selected_source(repo_dir, source, history=history, page_delay_ms=page_delay_ms, with_web=with_web)
            details.append(detail)
            success.append(source["id"])
            affected_dates.update(detail.get("dates") or [])
        except Exception as exc:
            failed.append({"id": source["id"], "error": str(exc)})
        finally:
            if idx < len(enabled_sources) - 1:
                _sleep_ms(page_delay_ms)

    return {
        "ok": not failed,
        "attempted_source_ids": attempted,
        "success_source_ids": success,
        "failed_sources": failed,
        "dates": sorted(affected_dates),
        "details": details,
    }


def sync_aihot_sources(
    repo_dir=ROOT,
    source_ids=None,
    with_web=False,
    history=False,
    page_delay_ms=DEFAULT_PAGE_DELAY_MS,
):
    report = sync_aihot_sources_report(
        repo_dir=repo_dir,
        source_ids=source_ids,
        with_web=with_web,
        history=history,
        page_delay_ms=page_delay_ms,
    )
    return report["dates"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-ids", help="comma-separated source ids to sync")
    ap.add_argument("--with-web", action="store_true", help="rebuild web/day JSON for affected dates")
    ap.add_argument("--history", action="store_true", help="backfill historical data instead of incremental sync")
    ap.add_argument("--page-delay-ms", type=int, default=DEFAULT_PAGE_DELAY_MS, help="delay between aihot API requests")
    ap.add_argument("--install-default-sources", action="store_true", help="install default 卡兹克 sources into config.local.json")
    args = ap.parse_args()
    if args.install_default_sources:
        result = ensure_default_aihot_sources(ROOT)
    else:
        source_ids = [part.strip() for part in str(args.source_ids or "").split(",") if part.strip()]
        result = sync_aihot_sources_report(
            repo_dir=ROOT,
            source_ids=source_ids,
            with_web=args.with_web,
            history=args.history,
            page_delay_ms=args.page_delay_ms,
        )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
