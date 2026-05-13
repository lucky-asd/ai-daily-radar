#!/usr/bin/env python3
"""Asynchronous tag derivation for AI Feed items."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import argparse
import json
import re
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timedelta, timezone

from pm_score import (
    CooldownRetryError,
    OpenAICompatibleScorer,
    _chunks,
    _extract_response_payload,
    representative_items_for_scoring,
)

TAG_VERSION = "v1"
TAG_STATUSES = ("done", "pending", "error", "inherited")
MAX_TAGS_PER_GROUP = 5
ENTITY_CANONICAL_MAP = {
    "openai": "OpenAI",
    "chatgpt": "ChatGPT",
    "gpt": "GPT",
    "gpt-4.1": "GPT-4.1",
    "gpt-image-v2": "GPT-image-V2",
    "gpt image v2": "GPT-image-V2",
    "gpt-image-2": "GPT-image-V2",
    "gpt image 2": "GPT-image-V2",
    "claude": "Claude",
    "anthropic": "Anthropic",
    "gemini": "Gemini",
    "google": "Google",
    "minimax": "MiniMax",
    "deepseek": "DeepSeek",
    "cursor": "Cursor",
    "windsurf": "Windsurf",
    "github": "GitHub",
    "microsoft": "Microsoft",
}
TOPIC_CANONICAL_MAP = {
    "model release": "模型发布",
    "模型更新": "模型发布",
    "模型上新": "模型发布",
    "image generation": "图像生成",
    "图像模型": "图像生成",
    "ai agent": "AI Agent",
    "agents": "AI Agent",
    "coding tool": "编程工具",
    "dev tool": "编程工具",
    "programming tool": "编程工具",
    "coding": "编程工具",
    "reasoning model": "推理模型",
}
_GENERIC_TAGS = {"ai", "人工智能", "模型", "产品", "公司", "平台", "工具", "news", "新闻"}


class TagPromptBuilder:
    def system_prompt(self):
        return (
            "你是 AI 新闻标签器。输出 JSON，对每条新闻给出 entity_tags、topic_tags、tag_reason。"
            "entity_tags 只放实体/产品/模型/公司/平台/项目名；topic_tags 只放主题/动作/领域。"
            "每组 1-5 个；尽量具体，避免'模型'、'产品'这类过泛词。"
            "topic_tags 优先使用像'模型发布'、'图像生成'、'AI Agent'、'编程工具'这样的组合词。"
            "tag_reason 用一句中文短句说明为什么打这些标签。"
        )

    def user_prompt(self, item):
        return json.dumps(
            {
                "item_id": item.get("item_id"),
                "title": item.get("title", ""),
                "summary": item.get("summary", ""),
                "source": item.get("source", ""),
                "category": item.get("category", ""),
            },
            ensure_ascii=False,
        )

    def batch_system_prompt(self):
        return (
            "你是 AI 新闻标签器。输出 JSON 数组，每项包含 item_id、entity_tags、topic_tags、tag_reason。"
            "entity_tags 只放实体/产品/模型/公司/平台/项目名；topic_tags 只放主题/动作/领域。"
            "每组 1-5 个；尽量具体，避免过泛词。"
        )

    def batch_user_prompt(self, items):
        return json.dumps(
            [
                {
                    "item_id": item.get("item_id"),
                    "title": item.get("title", ""),
                    "summary": item.get("summary", ""),
                    "source": item.get("source", ""),
                    "category": item.get("category", ""),
                }
                for item in items
            ],
            ensure_ascii=False,
        )


class OpenAICompatibleTagger(OpenAICompatibleScorer):
    def build_request_payload(self, item, builder=None):
        builder = builder or TagPromptBuilder()
        return {
            "model": self.profile["model"],
            "messages": [
                {"role": "system", "content": builder.system_prompt()},
                {"role": "user", "content": builder.user_prompt(item)},
            ],
            "response_format": {"type": "json_object"},
        }

    def build_batch_request_payload(self, items, builder=None):
        builder = builder or TagPromptBuilder()
        return {
            "model": self.profile["model"],
            "messages": [
                {"role": "system", "content": builder.batch_system_prompt()},
                {"role": "user", "content": builder.batch_user_prompt(items)},
            ],
        }

    def tag_one(self, item, builder=None):
        payload = self.build_request_payload(item, builder=builder)
        response = _extract_response_payload(self.transport(payload))
        normalized = _normalize_single_tag_result(response, item)
        return _finalize_tag_row(normalized, self.profile)

    def tag_batch(self, items, builder=None):
        payload = self.build_batch_request_payload(items, builder=builder)
        response = _extract_response_payload(self.transport(payload))
        normalized = _normalize_batch_tag_results(response, items)
        if not normalized:
            raise ValueError("batch tag response did not yield valid records")
        return [_finalize_tag_row(row, self.profile) for row in normalized.values()]

    def _error_result(self, item, exc):
        return {
            "item_id": item["item_id"],
            "entity_tags": [],
            "topic_tags": [],
            "tag_reason": None,
            "tag_status": "error",
            "tag_error": str(exc)[:300],
            "tag_model_profile": self.profile.get("id") or self.profile.get("name"),
            "tag_model": self.profile["model"],
            "tagged_at": datetime.now(timezone.utc).isoformat(),
            "inherited_from_item_id": None,
            "tag_version": TAG_VERSION,
        }

    def _tag_one_safe(self, item, builder=None):
        try:
            return self.tag_one(item, builder=builder)
        except CooldownRetryError:
            raise
        except Exception as exc:
            return self._error_result(item, exc)

    def _tag_batch_with_fallback(self, items, builder=None):
        if not items:
            return []
        if len(items) == 1:
            return [self._tag_one_safe(items[0], builder=builder)]
        try:
            batch_results = self.tag_batch(items, builder=builder)
        except CooldownRetryError:
            raise
        except Exception:
            midpoint = max(1, len(items) // 2)
            left = self._tag_batch_with_fallback(items[:midpoint], builder=builder)
            right = self._tag_batch_with_fallback(items[midpoint:], builder=builder)
            return left + right

        found = {row["item_id"] for row in batch_results}
        results = list(batch_results)
        for item in items:
            if item["item_id"] not in found:
                results.append(self._tag_one_safe(item, builder=builder))
        return results

    def tag_many(self, items, builder=None):
        if self.batch_size() <= 1:
            return [self._tag_one_safe(item, builder=builder) for item in items]
        results = []
        for batch in _chunks(items, self.batch_size()):
            results.extend(self._tag_batch_with_fallback(batch, builder=builder))
        return results


def _slug(text):
    text = str(text or "").strip().lower()
    text = text.replace("_", " ").replace("/", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_tag_list(value):
    if isinstance(value, str):
        parts = re.split(r"[,，、/\n]+", value)
    elif isinstance(value, list):
        parts = value
    else:
        parts = []
    cleaned = []
    seen = set()
    for part in parts:
        text = str(part or "").strip().strip("#")
        text = re.sub(r"^[\-•·]+", "", text).strip()
        if not text:
            continue
        text = re.sub(r"\s+", " ", text)
        slug = _slug(text)
        if not slug or slug in seen or slug in _GENERIC_TAGS:
            continue
        seen.add(slug)
        cleaned.append(text[:40])
        if len(cleaned) >= MAX_TAGS_PER_GROUP:
            break
    return cleaned


def _canonicalize_tag(text, mapping):
    raw = str(text or "").strip()
    if not raw:
        return None
    slug = _slug(raw)
    if slug in mapping:
        return mapping[slug]
    if re.fullmatch(r"[A-Za-z0-9 .+\-]{2,40}", raw):
        words = raw.split()
        if len(words) <= 3:
            return " ".join(word.upper() if word.isupper() else (word[:1].upper() + word[1:]) for word in words)
    return raw


def _normalize_reason(raw, item=None):
    text = str(raw or "").strip()
    if text:
        return text[:180]
    if item:
        return f"{item.get('source') or '这条新闻'}涉及明确实体和主题。"
    return None


def _normalize_single_tag_result(payload, item):
    if isinstance(payload, list):
        if len(payload) != 1:
            raise ValueError("single tag response unexpectedly returned multiple records")
        payload = payload[0]
    if not isinstance(payload, dict):
        raise ValueError("single tag response must be a json object")
    entity_tags = [_canonicalize_tag(tag, ENTITY_CANONICAL_MAP) for tag in _clean_tag_list(payload.get("entity_tags") or payload.get("entities"))]
    topic_tags = [_canonicalize_tag(tag, TOPIC_CANONICAL_MAP) for tag in _clean_tag_list(payload.get("topic_tags") or payload.get("topics"))]
    entity_tags = [tag for tag in entity_tags if tag]
    topic_tags = [tag for tag in topic_tags if tag]
    if not entity_tags and item.get("source"):
        entity_tags = [str(item.get("source"))]
    if not topic_tags:
        topic_tags = ["行业动态"]
    return {
        "item_id": item["item_id"],
        "entity_tags": entity_tags[:MAX_TAGS_PER_GROUP],
        "topic_tags": topic_tags[:MAX_TAGS_PER_GROUP],
        "tag_reason": _normalize_reason(payload.get("tag_reason") or payload.get("reason"), item=item),
    }


def _normalize_batch_tag_results(payload, items):
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            payload = payload["results"]
        elif all(isinstance(value, dict) for value in payload.values()):
            payload = [{"item_id": key, **value} for key, value in payload.items()]
        else:
            payload = [payload]
    if not isinstance(payload, list):
        raise ValueError("batch tag response must be a json array/object")
    items_by_id = {item["item_id"]: item for item in items if item.get("item_id")}
    normalized = {}
    item_ids = list(items_by_id.keys())
    for index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            continue
        item_id = entry.get("item_id") or (item_ids[index] if index < len(item_ids) else None)
        if item_id not in items_by_id:
            continue
        try:
            normalized[item_id] = _normalize_single_tag_result(entry, items_by_id[item_id])
        except ValueError:
            continue
    return normalized


def _finalize_tag_row(normalized, profile):
    return {
        "item_id": normalized["item_id"],
        "entity_tags": normalized.get("entity_tags") or [],
        "topic_tags": normalized.get("topic_tags") or [],
        "tag_reason": normalized.get("tag_reason"),
        "tag_status": "done",
        "tag_model_profile": profile.get("id") or profile.get("name"),
        "tag_model": profile.get("model"),
        "tagged_at": datetime.now(timezone.utc).isoformat(),
        "inherited_from_item_id": None,
        "tag_version": TAG_VERSION,
    }


def tag_file_path(repo_dir, date_str):
    return Path(repo_dir) / "data" / "ai-feed" / "tags" / f"{date_str}.json"


def _write_json_atomic(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp-{uuid.uuid4().hex[:8]}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_day_tags(path_or_repo, date_str=None):
    path = Path(path_or_repo) if date_str is None else tag_file_path(path_or_repo, date_str)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, dict) and isinstance(payload.get("items"), dict):
        return payload["items"]
    if isinstance(payload, dict):
        return payload
    return {}


def save_day_tags(path_or_repo, tags_or_date, maybe_tags=None):
    if maybe_tags is None:
        path = Path(path_or_repo)
        tags = tags_or_date
        date_str = path.stem
    else:
        path = tag_file_path(path_or_repo, tags_or_date)
        tags = maybe_tags
        date_str = tags_or_date
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "date": date_str,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "items": tags,
    }
    _write_json_atomic(path, payload)


def merge_tags_into_items(items, tags_by_id):
    merged = []
    for item in items:
        row = deepcopy(item)
        tags = tags_by_id.get(item.get("item_id"), {})
        row["entity_tags"] = list(tags.get("entity_tags") or [])
        row["topic_tags"] = list(tags.get("topic_tags") or [])
        row["tag_reason"] = tags.get("tag_reason")
        row["tag_status"] = tags.get("tag_status")
        row["tag_model_profile"] = tags.get("tag_model_profile")
        row["tag_model"] = tags.get("tag_model")
        row["tagged_at"] = tags.get("tagged_at")
        row["inherited_from_item_id"] = tags.get("inherited_from_item_id")
        row["tag_version"] = tags.get("tag_version")
        merged.append(row)
    return merged


def _day_path(repo_dir, date_str):
    return Path(repo_dir) / "web" / "data" / "day" / f"{date_str}.json"


def _build_cluster_map(day_payload):
    items = day_payload.get("items", [])
    clusters = ((day_payload.get("clusters") or {}).get("auto") or [])
    rep_to_members = {}
    member_to_rep = {}
    for cluster in clusters:
        rep_item = items[cluster.get("rep", -1)] if 0 <= cluster.get("rep", -1) < len(items) else None
        if not rep_item or not rep_item.get("item_id"):
            continue
        rep_id = rep_item["item_id"]
        member_ids = []
        for idx in cluster.get("members", []):
            if not (0 <= idx < len(items)):
                continue
            item = items[idx]
            if not item.get("item_id"):
                continue
            member_ids.append(item["item_id"])
            member_to_rep[item["item_id"]] = rep_id
        rep_to_members[rep_id] = member_ids
    return rep_to_members, member_to_rep


def _inherit_cluster_tags(records, rep_to_members, allow_inherit=True, force=False):
    if not allow_inherit:
        return 0
    inherited = 0
    for rep_id, member_ids in rep_to_members.items():
        rep_row = records.get(rep_id) or {}
        if rep_row.get("tag_status") != "done":
            continue
        for member_id in member_ids:
            if member_id == rep_id:
                continue
            existing = records.get(member_id) or {}
            if (not force) and existing.get("tag_status") in {"done", "inherited"}:
                continue
            records[member_id] = {
                "item_id": member_id,
                "entity_tags": list(rep_row.get("entity_tags") or []),
                "topic_tags": list(rep_row.get("topic_tags") or []),
                "tag_reason": rep_row.get("tag_reason"),
                "tag_status": "inherited",
                "tag_model_profile": rep_row.get("tag_model_profile"),
                "tag_model": rep_row.get("tag_model"),
                "tagged_at": datetime.now(timezone.utc).isoformat(),
                "inherited_from_item_id": rep_id,
                "tag_version": rep_row.get("tag_version") or TAG_VERSION,
            }
            inherited += 1
    return inherited


def _count_records(records, target_ids=None):
    counts = {"total": 0, "done": 0, "pending": 0, "error": 0, "inherited": 0, "skipped": 0}
    for item_id in target_ids or records.keys():
        row = records.get(item_id) or {}
        counts["total"] += 1
        status = row.get("tag_status")
        if status in counts:
            counts[status] += 1
    return counts


def tag_date(repo_dir, date_str, profile, *, rebuild_day=None, scorer_factory=None, force=False, settings=None):
    repo_dir = Path(repo_dir)
    day_path = _day_path(repo_dir, date_str)
    if not day_path.exists():
        return {"processed": 0, "counts": {"total": 0, "done": 0, "pending": 0, "error": 0, "inherited": 0, "skipped": 0}}
    day_payload = json.loads(day_path.read_text(encoding="utf-8") or "{}")
    day_items = [item for item in day_payload.get("items", []) if item.get("item_id")]
    if not day_items:
        return {"processed": 0, "counts": {"total": 0, "done": 0, "pending": 0, "error": 0, "inherited": 0, "skipped": 0}}
    settings = settings or {}
    allow_inherit = settings.get("allow_inherit_from_cluster", True)
    selected = [item for item in representative_items_for_scoring(day_payload, profile="auto") if item.get("item_id")]
    raw_max_pending = settings.get("max_pending_per_run")
    try:
        raw_max_pending = int(raw_max_pending)
    except (TypeError, ValueError):
        raw_max_pending = None
    max_pending_per_run = len(selected) if raw_max_pending in (None, 0) else max(1, raw_max_pending)
    rep_to_members, member_to_rep = _build_cluster_map(day_payload)
    selected_ids = [item["item_id"] for item in selected]
    all_item_ids = [item["item_id"] for item in day_items]
    existing = {item_id: row for item_id, row in load_day_tags(repo_dir, date_str).items() if item_id in all_item_ids}

    skipped = 0
    _inherit_cluster_tags(existing, rep_to_members, allow_inherit=allow_inherit, force=force)
    to_tag = []
    for item in selected:
        row = existing.get(item["item_id"]) or {}
        if (not force) and row.get("tag_status") == "done":
            skipped += 1
            continue
        if (not force) and row.get("tag_status") == "inherited" and item["item_id"] not in member_to_rep:
            skipped += 1
            continue
        if len(to_tag) >= max_pending_per_run:
            break
        existing[item["item_id"]] = {
            **row,
            "item_id": item["item_id"],
            "tag_status": "pending",
            "tag_model_profile": profile.get("id") or profile.get("name"),
            "tag_model": profile.get("model"),
            "tag_version": TAG_VERSION,
        }
        to_tag.append(item)
    save_day_tags(repo_dir, date_str, existing)
    if not to_tag:
        if allow_inherit:
            _inherit_cluster_tags(existing, rep_to_members, allow_inherit=True, force=force)
            save_day_tags(repo_dir, date_str, existing)
            if rebuild_day:
                rebuild_day(date_str)
        counts = _count_records(existing, target_ids=all_item_ids)
        counts["skipped"] = skipped
        return {"processed": 0, "counts": counts}

    scorer_factory = scorer_factory or (lambda active_profile: OpenAICompatibleTagger(active_profile))
    rebuild_day = rebuild_day or (
        lambda target_date: subprocess.run(
            [sys.executable, str(repo_dir / "scripts" / "build-web-data.py"), "--date", target_date],
            check=True,
        )
    )
    processed = 0
    raw_batch_size = settings.get("batch_size")
    if raw_batch_size is None:
        batch_size = getattr(scorer_factory(profile), "batch_size", lambda: int(profile.get("batch_size") or 1))()
    else:
        try:
            batch_size = int(raw_batch_size)
        except (TypeError, ValueError):
            batch_size = getattr(scorer_factory(profile), "batch_size", lambda: int(profile.get("batch_size") or 1))()
        batch_size = max(1, min(10, batch_size))
    raw_parallel_workers = settings.get("parallel_workers", 1)
    try:
        parallel_workers = int(raw_parallel_workers)
    except (TypeError, ValueError):
        parallel_workers = 1
    parallel_workers = max(1, min(5, parallel_workers))
    batches = list(_chunks(to_tag, batch_size))

    def apply_batch_results(batch, results):
        nonlocal processed
        found = {row["item_id"] for row in results}
        for result in results:
            existing[result["item_id"]] = result
            processed += 1
        missing = [item for item in batch if item["item_id"] not in found]
        for item in missing:
            existing[item["item_id"]] = {
                **existing.get(item["item_id"], {"item_id": item["item_id"]}),
                "tag_status": "error",
                "tag_reason": None,
                "tag_error": "missing result from tagger",
                "tag_model_profile": profile.get("id") or profile.get("name"),
                "tag_model": profile.get("model"),
                "tag_version": TAG_VERSION,
            }
        _inherit_cluster_tags(existing, rep_to_members, allow_inherit=allow_inherit, force=force)
        save_day_tags(repo_dir, date_str, existing)

    if parallel_workers <= 1 or len(batches) <= 1:
        tagger = scorer_factory(profile)
        for batch in batches:
            try:
                results = tagger.tag_many(batch)
                apply_batch_results(batch, results)
            except CooldownRetryError:
                save_day_tags(repo_dir, date_str, existing)
                raise
            except Exception as exc:
                error_text = str(exc)[:300]
                for item in batch:
                    existing[item["item_id"]] = {
                        **existing.get(item["item_id"], {"item_id": item["item_id"]}),
                        "tag_status": "error",
                        "tag_reason": None,
                        "tag_error": error_text,
                        "tag_model_profile": profile.get("id") or profile.get("name"),
                        "tag_model": profile.get("model"),
                        "tag_version": TAG_VERSION,
                    }
                _inherit_cluster_tags(existing, rep_to_members, allow_inherit=allow_inherit, force=force)
                save_day_tags(repo_dir, date_str, existing)
    else:
        cooldown_exc = None
        worker_count = min(parallel_workers, len(batches))

        def run_batch(batch):
            tagger = scorer_factory(profile)
            return tagger.tag_many(batch)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_batch = {executor.submit(run_batch, batch): batch for batch in batches}
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    results = future.result()
                    apply_batch_results(batch, results)
                except CooldownRetryError as exc:
                    cooldown_exc = cooldown_exc or exc
                except Exception as exc:
                    error_text = str(exc)[:300]
                    for item in batch:
                        existing[item["item_id"]] = {
                            **existing.get(item["item_id"], {"item_id": item["item_id"]}),
                            "tag_status": "error",
                            "tag_reason": None,
                            "tag_error": error_text,
                            "tag_model_profile": profile.get("id") or profile.get("name"),
                            "tag_model": profile.get("model"),
                            "tag_version": TAG_VERSION,
                        }
                    _inherit_cluster_tags(existing, rep_to_members, allow_inherit=allow_inherit, force=force)
                    save_day_tags(repo_dir, date_str, existing)
        if cooldown_exc:
            save_day_tags(repo_dir, date_str, existing)
            raise cooldown_exc
    rebuild_day(date_str)
    counts = _count_records(existing, target_ids=all_item_ids)
    counts["skipped"] = skipped
    return {"processed": processed, "counts": counts}


def tag_dates(repo_dir, dates, profile, *, rebuild_day=None, scorer_factory=None, force=False, settings=None):
    total = 0
    outcomes = []
    for date_str in dates:
        result = tag_date(
            repo_dir,
            date_str,
            profile,
            rebuild_day=rebuild_day,
            scorer_factory=scorer_factory,
            force=force,
            settings=settings,
        )
        total += int(result.get("processed") or 0)
        outcomes.append({"date": date_str, **result})
    return {"processed": total, "dates": outcomes}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=str(Path(__file__).resolve().parent.parent))
    ap.add_argument("--date")
    ap.add_argument("--from-date")
    ap.add_argument("--to-date")
    ap.add_argument("--profile-json")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    if not args.profile_json:
        raise SystemExit("--profile-json is required")
    profile = json.loads(args.profile_json)
    if args.date:
        dates = [args.date]
    else:
        if not args.from_date or not args.to_date:
            raise SystemExit("provide --date or --from-date/--to-date")
        start = datetime.fromisoformat(args.from_date)
        end = datetime.fromisoformat(args.to_date)
        cursor = start
        dates = []
        while cursor <= end:
            dates.append(cursor.strftime("%Y-%m-%d"))
            cursor = cursor + timedelta(days=1)
    result = tag_dates(args.repo, dates, profile, force=args.force)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
