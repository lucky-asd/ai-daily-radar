#!/usr/bin/env python3
"""Fetch configured RSS sources into atomic JSONL files."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib import request as urllib_request
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from rss_labels import normalize_rss_category, normalize_rss_config, rss_source_label
from runtime_config import RuntimeConfigStore

ROOT = SCRIPT_DIR.parent
DEFAULT_CATEGORY = "📦 其他"
DEFAULT_LOCAL_DELAY_MS = 180
DEFAULT_REMOTE_DELAY_MS = 900
PLACEHOLDER_DATE_STRINGS = {"2001-01-01"}


def _local_name(tag):
    return str(tag).split("}", 1)[-1] if tag else ""


def _child_text(node, *names):
    wanted = set(names)
    for child in list(node):
        if _local_name(child.tag) in wanted:
            text = "".join(child.itertext()).strip()
            if text:
                return text
    return ""


def _child_link(node):
    for child in list(node):
        if _local_name(child.tag) != "link":
            continue
        href = (child.attrib.get("href") or "").strip()
        if href:
            return href
        text = "".join(child.itertext()).strip()
        if text:
            return text
    return ""


def _parse_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = parsedate_to_datetime(text)
        if dt is not None:
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError, IndexError, OverflowError):
        pass
    normalized = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _read_feed_text(url, timeout=20):
    req = urllib_request.Request(url, headers={"User-Agent": "AI-Feed-RSS/1.0"})
    with urllib_request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _is_local_feed_url(url):
    parsed = urlparse(str(url or "").strip())
    host = (parsed.hostname or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "0.0.0.0"}


def parse_feed_entries(url):
    raw = _read_feed_text(url).lstrip()
    root = ET.fromstring(raw)
    tag = _local_name(root.tag)
    if tag == "rss":
        channel = next((child for child in list(root) if _local_name(child.tag) == "channel"), root)
        entries = [child for child in list(channel) if _local_name(child.tag) == "item"]
    elif tag == "feed":
        entries = [child for child in list(root) if _local_name(child.tag) == "entry"]
    else:
        entries = [child for child in root.iter() if _local_name(child.tag) in {"item", "entry"}]

    parsed = []
    for entry in entries:
        title = _child_text(entry, "title") or "(无标题)"
        link = _child_link(entry)
        summary = _child_text(entry, "description", "summary", "content")
        author = _child_text(entry, "author", "creator", "name")
        published_raw = _child_text(entry, "pubDate", "published", "updated")
        published_at = _parse_datetime(published_raw) or datetime.now(timezone.utc)
        guid = _child_text(entry, "guid", "id")
        stable_key = guid or link or f"{title}|{published_at.isoformat()}"
        parsed.append({
            "title": title.strip(),
            "link": link.strip(),
            "summary": re.sub(r"\s+", " ", summary).strip(),
            "author": author.strip(),
            "published_at": published_at,
            "stable_key": stable_key.strip(),
        })
    return parsed


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


def _rows_changed(existing, merged):
    """Avoid treating every RSS poll as a change when content is identical."""
    return existing != merged


def _item_id(source_id, stable_key):
    digest = hashlib.sha1(f"{source_id}|{stable_key}".encode("utf-8")).hexdigest()[:16]
    return f"{source_id}::{digest}"


def _save_source_day(repo_dir, source, date_str, items):
    source_id = source["id"]
    config = normalize_rss_config(source.get("config", {}) or {})
    feed_url = config.get("feed_url") or ""
    source_name = str(config.get("source_label") or "").strip() or rss_source_label(feed_url)
    category = normalize_rss_category(config.get("category"), feed_url=feed_url) or DEFAULT_CATEGORY
    card_id = f"rss::{source_id}::{date_str}"
    item_rows = []
    for item in items:
        item_rows.append({
            "item_id": _item_id(source_id, item["stable_key"]),
            "category": category,
            "raw_cat": category,
            "title": item["title"],
            "summary": item["summary"],
            "author": item["author"],
            "url": item["link"],
            "source": source_name,
            "segment": "RSS",
            "card_msg_id": card_id,
            "published_at": item["published_at"].isoformat(),
            "date": date_str,
        })
    card_rows = [{
        "message_id": card_id,
        "title": f"{source.get('name') or source_name} RSS · {date_str}",
        "source": source_name,
        "count": len(item_rows),
        "segment": "RSS",
        "items_count": len(item_rows),
        "overview": f"{category}：{len(item_rows)} 条",
        "date": date_str,
    }]

    cards_path = Path(repo_dir) / "data" / source_id / "cards" / f"{date_str}.jsonl"
    items_path = Path(repo_dir) / "data" / source_id / "items" / f"{date_str}.jsonl"
    existing_cards = _read_jsonl(cards_path)
    existing_items = _read_jsonl(items_path)
    merged_cards = _merge_rows(existing_cards, card_rows, "message_id")
    merged_items = _merge_rows(existing_items, item_rows, "item_id")
    changed = _rows_changed(existing_cards, merged_cards) or _rows_changed(existing_items, merged_items)
    if changed:
        _write_jsonl(cards_path, merged_cards)
        _write_jsonl(items_path, merged_items)
    return changed


def _is_placeholder_date(dt):
    if not dt:
        return False
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d") in PLACEHOLDER_DATE_STRINGS


def _rebuild_web_day(repo_dir, date_str):
    subprocess.run([sys.executable, str(Path(repo_dir) / "scripts" / "build-web-data.py"), "--date", date_str], check=True)


def sync_rss_sources_report(
    repo_dir=ROOT,
    source_ids=None,
    with_web=False,
    local_delay_ms=DEFAULT_LOCAL_DELAY_MS,
    remote_delay_ms=DEFAULT_REMOTE_DELAY_MS,
    max_age_days=None,
):
    repo_dir = Path(repo_dir)
    cfg = RuntimeConfigStore(repo_dir / "config.yaml", repo_dir / "config.local.json").load()
    enabled_sources = []
    source_filter = {str(source_id).strip() for source_id in (source_ids or []) if str(source_id).strip()}
    for source in cfg.get("sources", []) or []:
        if source.get("type") != "rss" or not source.get("enabled", True):
            continue
        if source_filter and source.get("id") not in source_filter:
            continue
        feed_url = str((source.get("config") or {}).get("feed_url") or "").strip()
        if not feed_url:
            continue
        enabled_sources.append(source)

    affected_dates = set()
    cutoff = None
    if max_age_days is not None and int(max_age_days) > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(max_age_days))
    attempted_source_ids = []
    success_source_ids = []
    failed_sources = []
    total = len(enabled_sources)
    for index, source in enumerate(enabled_sources):
        source_id = source.get("id")
        attempted_source_ids.append(source_id)
        feed_url = source.get("config", {}).get("feed_url")
        try:
            entries = parse_feed_entries(feed_url)
            by_date = defaultdict(list)
            for item in entries:
                if _is_placeholder_date(item.get("published_at")):
                    continue
                if cutoff and item.get("published_at") and item["published_at"] < cutoff:
                    continue
                date_str = item["published_at"].astimezone(timezone.utc).strftime("%Y-%m-%d")
                by_date[date_str].append(item)
            for date_str, day_items in by_date.items():
                changed = _save_source_day(repo_dir, source, date_str, day_items)
                if not changed:
                    continue
                affected_dates.add(date_str)
                if with_web:
                    _rebuild_web_day(repo_dir, date_str)
            success_source_ids.append(source_id)
        except Exception as exc:
            failed_sources.append({
                "id": source_id,
                "url": feed_url,
                "error": str(exc),
            })
        finally:
            if index < total - 1:
                delay_ms = local_delay_ms if _is_local_feed_url(feed_url) else remote_delay_ms
                if delay_ms and delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
    return {
        "ok": not failed_sources,
        "dates": sorted(affected_dates),
        "attempted_source_ids": attempted_source_ids,
        "success_source_ids": success_source_ids,
        "failed_sources": failed_sources,
    }


def sync_rss_sources(
    repo_dir=ROOT,
    source_ids=None,
    with_web=False,
    local_delay_ms=DEFAULT_LOCAL_DELAY_MS,
    remote_delay_ms=DEFAULT_REMOTE_DELAY_MS,
    max_age_days=None,
):
    report = sync_rss_sources_report(
        repo_dir=repo_dir,
        source_ids=source_ids,
        with_web=with_web,
        local_delay_ms=local_delay_ms,
        remote_delay_ms=remote_delay_ms,
        max_age_days=max_age_days,
    )
    return report["dates"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-ids", help="comma-separated source ids to sync")
    ap.add_argument("--with-web", action="store_true", help="rebuild web/day JSON for affected dates")
    ap.add_argument("--local-delay-ms", type=int, default=DEFAULT_LOCAL_DELAY_MS, help="delay between local feed sources")
    ap.add_argument("--remote-delay-ms", type=int, default=DEFAULT_REMOTE_DELAY_MS, help="delay between remote feed sources")
    ap.add_argument("--max-age-days", type=int, help="ignore RSS entries older than this many days")
    args = ap.parse_args()
    source_ids = [part.strip() for part in str(args.source_ids or "").split(",") if part.strip()]
    result = sync_rss_sources_report(
        repo_dir=ROOT,
        source_ids=source_ids,
        with_web=args.with_web,
        local_delay_ms=args.local_delay_ms,
        remote_delay_ms=args.remote_delay_ms,
        max_age_days=args.max_age_days,
    )
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
