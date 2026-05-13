#!/usr/bin/env python3
"""Build the static JSON the website consumes.

Reads JSONL from data/<source_id>/{cards,items}/YYYY-MM-DD.jsonl for every
enabled source in config.yaml, and writes:

  web/data/index.json            # manifest: sources, categories, days
  web/data/day/YYYY-MM-DD.json   # all items+cards for that day, merged across sources

Run standalone or at the tail of scripts/sync-ai-feed.py.

Usage:
    python3 scripts/build-web-data.py                 # rebuild everything
    python3 scripts/build-web-data.py --date 2026-04-17  # one day only
"""
import argparse, json, re, sys, uuid
from collections import defaultdict
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
from cluster import cluster_items, summarize_cluster  # noqa: E402
from pm_score import load_day_scores, merge_scores_into_items  # noqa: E402
from rss_labels import normalize_rss_category, rss_source_label  # noqa: E402
from runtime_config import RuntimeConfigStore  # noqa: E402
from tagging import load_day_tags, merge_tags_into_items  # noqa: E402

ROOT = SCRIPT_DIR.parent
CONFIG = ROOT / "config.yaml"
DATA = ROOT / "data"
SCORES = DATA / "ai-feed" / "scores"
TAGS = DATA / "ai-feed" / "tags"


def _write_text_atomic(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex[:8]}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def load_config():
    return RuntimeConfigStore(CONFIG, ROOT / "config.local.json").load()


def _parse_scalar(v):
    v = v.strip()
    if v in ("true", "false"):
        return v == "true"
    if v.startswith("[") and v.endswith("]"):
        return [x.strip() for x in v[1:-1].split(",") if x.strip()]
    if v.startswith('"') and v.endswith('"'):
        return v[1:-1]
    return v


def read_jsonl(p: Path):
    if not p.exists():
        return []
    out = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def _normalize_item_for_web(item, source):
    row = dict(item)
    if (source or {}).get("type") != "rss":
        return row
    config = (source or {}).get("config") or {}
    feed_url = str(config.get("feed_url") or "").strip()
    category = normalize_rss_category(row.get("category") or config.get("category"), feed_url=feed_url)
    row["category"] = category
    row["raw_cat"] = category
    row["source"] = str(config.get("source_label") or "").strip() or rss_source_label(feed_url)
    return row


def _normalize_card_for_web(card, source):
    row = dict(card)
    if (source or {}).get("type") != "rss":
        return row
    config = (source or {}).get("config") or {}
    feed_url = str(config.get("feed_url") or "").strip()
    category = normalize_rss_category(config.get("category"), feed_url=feed_url)
    row["source"] = str(config.get("source_label") or "").strip() or rss_source_label(feed_url)
    if row.get("overview"):
        count = row.get("count") or row.get("items_count")
        if count is not None:
            row["overview"] = f"{category}：{count} 条"
    return row


def build(date_filter=None):
    cfg = load_config()
    out_dir = ROOT / (cfg.get("outputs", {}).get("web", {}).get("out_dir") or "web/data")
    day_dir = out_dir / "day"
    out_dir.mkdir(parents=True, exist_ok=True)
    day_dir.mkdir(parents=True, exist_ok=True)

    sources = [s for s in cfg.get("sources", []) if s.get("enabled", True) and "web" in (s.get("derived") or ["web"])]
    categories = cfg.get("categories", [])

    # Discover all dates present in any enabled source.
    dates = set()
    for s in sources:
        items_dir = DATA / s["id"] / "items"
        if items_dir.exists():
            for p in items_dir.glob("*.jsonl"):
                dates.add(p.stem)
    all_dates = sorted(dates, reverse=True)
    target_dates = [date_filter] if date_filter else all_dates

    # Per-day JSON.
    day_meta = {}
    for d in target_dates:
        merged_items, merged_cards, cat_counts, src_counts = [], [], defaultdict(int), defaultdict(int)
        for s in sources:
            items = read_jsonl(DATA / s["id"] / "items" / f"{d}.jsonl")
            cards = read_jsonl(DATA / s["id"] / "cards" / f"{d}.jsonl")
            for it in items:
                it = _normalize_item_for_web(it, s)
                it["_source"] = s["id"]
                merged_items.append(it)
                cat_counts[it.get("category", "📦 其他")] += 1
                src_counts[it.get("source", s["id"])] += 1
            for c in cards:
                c = _normalize_card_for_web(c, s)
                c["_source"] = s["id"]
                merged_cards.append(c)
        day_scores = load_day_scores(SCORES / f"{d}.json")
        day_tags = load_day_tags(TAGS / f"{d}.json")
        merged_items = merge_scores_into_items(merged_items, day_scores)
        merged_items = merge_tags_into_items(merged_items, day_tags)
        tagged_items = sum(1 for item in merged_items if item.get("tag_status") in {"done", "inherited"})
        _write_text_atomic(day_dir / f"{d}.json", json.dumps({
            "date": d,
            "cards": merged_cards,
            "items": merged_items,
            "clusters": _build_clusters(merged_items),
        }, ensure_ascii=False, separators=(",", ":")))
        day_meta[d] = {
            "date": d,
            "items": len(merged_items),
            "cards": len(merged_cards),
            "scored_items": sum(1 for item in merged_items if item.get("pm_label")),
            "tagged_items": tagged_items,
            "categories": dict(cat_counts),
            "sources": dict(src_counts),
        }

    # Index.json — merge prior meta if we only rebuilt one date.
    index_path = out_dir / "index.json"
    if date_filter and index_path.exists():
        prior = json.loads(index_path.read_text(encoding="utf-8"))
        prior_days = {d["date"]: d for d in prior.get("days", [])}
        prior_days.update(day_meta)
        days = sorted(prior_days.values(), key=lambda x: x["date"], reverse=True)
    else:
        days = sorted(day_meta.values(), key=lambda x: x["date"], reverse=True)

    index = {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "sources": [{"id": s["id"], "name": s.get("name", s["id"])} for s in sources],
        "categories": categories,
        "days": days,
    }
    _write_text_atomic(index_path, json.dumps(index, ensure_ascii=False, separators=(",", ":")))
    print(f"built {len(target_dates)} day file(s); index has {len(days)} days")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="build a single YYYY-MM-DD")
    args = ap.parse_args()
    build(args.date)


def _build_clusters(items):
    """Produce both 'auto' and 'aggressive' clusterings for each day.

    Output shape:
      {
        "auto":       [ {rep_index, member_indices, size, sources}, ... ],
        "aggressive": [ ... ],
      }
    Only clusters with size >= 2 are listed — singletons are implicit.
    """
    out = {}
    for profile in ("auto", "aggressive"):
        groups = cluster_items(items, profile=profile)
        summaries = []
        for g in groups:
            if len(g) < 2:
                continue
            s = summarize_cluster(items, g)
            # Drop the id-based fields; indices are enough for the client.
            summaries.append({
                "rep": s["rep_index"],
                "members": s["member_indices"],
                "size": s["size"],
                "sources": s["sources"],
            })
        # Sort descending by size so the client can render largest first.
        summaries.sort(key=lambda x: (-x["size"], x["rep"]))
        out[profile] = summaries
    return out


if __name__ == "__main__":
    main()
