#!/usr/bin/env python3
"""PM news scoring storage + lightweight OpenAI-compatible scorer."""

from __future__ import annotations

import json
import re
import shutil
import ssl
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.error
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


VALID_PM_LABELS = ("必读", "值得读", "可选读", "略过")
DEFAULT_PM_SCORES = {
    "必读": 95,
    "值得读": 82,
    "可选读": 65,
    "略过": 35,
}
LABEL_ALIASES = {
    "must_read": "必读",
    "must-read": "必读",
    "high": "必读",
    "recommended": "值得读",
    "worth_reading": "值得读",
    "worth-reading": "值得读",
    "worth": "值得读",
    "optional": "可选读",
    "medium": "可选读",
    "skip": "略过",
    "ignore": "略过",
    "low": "略过",
}
LEVEL_ALIASES = {
    "low": "low",
    "弱": "low",
    "偏弱": "low",
    "低": "low",
    "低价值": "low",
    "medium": "medium",
    "mid": "medium",
    "中": "medium",
    "中等": "medium",
    "一般": "medium",
    "中等价值": "medium",
    "high": "high",
    "强": "high",
    "偏强": "high",
    "高": "high",
    "高价值": "high",
}
LEVEL_SCORE = {"low": 0, "medium": 1, "high": 2}
LEVEL_FIELDS = {
    "pm_signal_level": ("pm_signal_level", "signal_level", "signal_density_level"),
    "pm_decision_level": ("pm_decision_level", "decision_level", "decision_value_level"),
    "pm_transfer_level": ("pm_transfer_level", "transfer_level", "transfer_value_level"),
    "pm_evidence_level": ("pm_evidence_level", "evidence_level", "evidence_strength_level"),
    "pm_constraint_level": ("pm_constraint_level", "constraint_level", "constraint_awareness_level"),
}
MIN_SCORABLE_SUMMARY_CHARS = 15


class CooldownRetryError(RuntimeError):
    def __init__(self, message, retry_after_seconds=None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def _extract_retry_after_seconds(text):
    text = str(text or "")
    patterns = [
        r"retry[-_\s]?after[^0-9]{0,10}(\d+)",
        r"after[^0-9]{0,10}(\d+)\s*(?:seconds|second|secs|sec|s|分钟|分|minutes|minute)",
        r"(\d+)\s*(?:seconds|second|secs|sec)\b",
        r"(\d+)\s*(?:minutes|minute|mins|min)\b",
        r"(\d+)\s*(?:分钟|分)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = int(match.group(1))
        if "min" in pattern or "分钟" in pattern or "分" in pattern:
            return value * 60
        if "after" in pattern and re.search(r"minutes|minute|mins|min|分钟|分", match.group(0), flags=re.IGNORECASE):
            return value * 60
        return value
    return None


def _is_rate_limited(status=None, text=""):
    body = str(text or "").lower()
    if status == 429:
        return True
    return any(token in body for token in (
        "rate limit",
        "too many requests",
        "quota",
        "cooldown",
        "rate_limit",
        "limit reached",
        "please retry later",
        "try again later",
        "额度",
        "限流",
        "冷却",
        "请求过于频繁",
    ))


def _is_transient_transport_error(exc=None, text=""):
    body = f"{exc or ''}\n{text or ''}".lower()
    return any(token in body for token in (
        "connection refused",
        "failed to connect",
        "connection reset",
        "connection aborted",
        "remote end closed connection",
        "temporarily unavailable",
        "timed out",
        "timeout",
        "read operation timed out",
        "network is unreachable",
        "name or service not known",
        "nodename nor servname provided",
        "could not resolve host",
        "curl: (7)",
        "curl: (28)",
        "http status: 000",
    ))


def _raise_transport_error(status, body):
    if _is_rate_limited(status, body):
        raise CooldownRetryError(body or f"http error {status}", retry_after_seconds=_extract_retry_after_seconds(body))
    if status in {0, 408, 425, 500, 502, 503, 504} or _is_transient_transport_error(text=body):
        raise CooldownRetryError(body or f"temporary http error {status}")
    raise RuntimeError(body or f"http error {status}")


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


def _strip_html(text):
    text = str(text or "")
    if not text:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n", text)
    text = _HTML_TAG_RE.sub(" ", text)
    text = text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&#8217;", "’")
    return _WHITESPACE_RE.sub(" ", text).strip()


def _truncate_text(text, limit):
    text = str(text or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit) - 1)].rstrip() + "…"


def build_scoring_context(item):
    item = item or {}
    source_id = str(item.get("_source") or "").strip()
    category = str(item.get("category") or "").strip()
    source_label = str(item.get("source") or "").strip()
    url = str(item.get("url") or "").strip()
    domain = urlparse(url).netloc if url else ""
    raw_summary = _strip_html(item.get("summary") or "")
    title = str(item.get("title") or "").strip()
    summary_len = len(raw_summary)

    if source_id in {"aihot-selected", "aihot-daily"} or category in {"卡兹克精选", "卡兹克日报"}:
        source_kind = "kazike"
        scoring_mode = "brief_news"
        summary_limit = 360
    elif category == "公众号" or "mp.weixin.qq.com" in domain:
        source_kind = "wechat_article"
        scoring_mode = "article_digest" if summary_len >= 60 else "title_only_article"
        summary_limit = 620
    elif category == "RSS源" or source_label == "RSS源" or source_id.startswith("rss-"):
        source_kind = "rss_article"
        scoring_mode = "article_digest" if summary_len >= 80 else "title_only_article"
        summary_limit = 620
    else:
        source_kind = "generic"
        scoring_mode = "brief_news" if summary_len and summary_len <= 240 else "article_digest"
        summary_limit = 420

    context_quality = "good" if summary_len >= 60 else ("ok" if summary_len >= 20 else "thin")
    if scoring_mode == "brief_news" and summary_len >= 20:
        context_quality = "good"
    elif scoring_mode == "title_only_article":
        context_quality = "thin"

    return {
        "source_kind": source_kind,
        "scoring_mode": scoring_mode,
        "context_quality": context_quality,
        "summary_length": summary_len,
        "title_length": len(title),
        "summary_for_model": _truncate_text(raw_summary, summary_limit),
        "domain": domain,
    }


class PromptBuilder:
    def system_prompt(self):
        return (
            "你是产品经理阅读价值评估器。不要直接输出阅读标签，只做稳定的粗粒度评估。"
            "输出 JSON，字段仅包含 signal_level, decision_value_level, transfer_value_level, evidence_strength_level, constraint_level, pm_reason。"
            "五个 level 只能是 low / medium / high。"
            "你会收到 scoring_mode、source_kind、context_quality。"
            "如果 scoring_mode=brief_news，就按新闻摘要来判断，不要强行按长文深度解读。"
            "如果 scoring_mode=article_digest，就把 summary_for_model 当成长文导读/文章提要，重点判断核心观点、证据、对产品/工作流的影响。"
            "如果 scoring_mode=title_only_article 或 context_quality=thin，说明信息明显不足；除非标题已经明确写出官方发布、定价/API/规则变化、产品能力更新，否则 signal_level 和 decision_value_level 不得高于 low，transfer_value_level 不得高于 medium。"
            "校准规则："
            "如果主要是传闻、单条社媒观点、主观体验，且缺少明确数据或官方动作，则 signal_level 和 decision_value_level 不得高于 medium。"
            "如果只是品牌更名、普通融资、估值、会见、宣传预告，没有明确产品/平台规则变化，则 decision_value_level 不得高于 low。"
            "如果只有技术炫技，没有清晰产品场景或工作流影响，则 transfer_value_level 不得高于 medium。"
            "pm_reason 保持一句中文短句，必须具体说明为什么对产品经理有或没有价值。"
        )

    def user_prompt(self, item):
        context = build_scoring_context(item)
        return json.dumps(
            {
                "item_id": item.get("item_id", ""),
                "title": item.get("title", ""),
                "summary": context["summary_for_model"],
                "summary_for_model": context["summary_for_model"],
                "source": item.get("source", ""),
                "category": item.get("category", ""),
                "source_kind": context["source_kind"],
                "scoring_mode": context["scoring_mode"],
                "context_quality": context["context_quality"],
                "summary_length": context["summary_length"],
                "domain": context["domain"],
            },
            ensure_ascii=False,
        )

    def batch_system_prompt(self):
        return (
            "你是产品经理阅读价值评估器。不要直接输出阅读标签，只做稳定的粗粒度评估。"
            "输出 JSON 数组；每一项仅包含 item_id, signal_level, decision_value_level, transfer_value_level, evidence_strength_level, constraint_level, pm_reason。"
            "五个 level 只能是 low / medium / high。"
            "你会收到 scoring_mode、source_kind、context_quality。"
            "brief_news 按新闻摘要判断；article_digest 按长文提要判断；title_only_article/context_quality=thin 时务必保守，不要因为标题像大词就打高分。"
            "校准规则："
            "如果主要是传闻、单条社媒观点、主观体验，且缺少明确数据或官方动作，则 signal_level 和 decision_value_level 不得高于 medium。"
            "如果只是品牌更名、普通融资、估值、会见、宣传预告，没有明确产品/平台规则变化，则 decision_value_level 不得高于 low。"
            "如果只有技术炫技，没有清晰产品场景或工作流影响，则 transfer_value_level 不得高于 medium。"
            "pm_reason 保持一句中文短句，必须具体说明为什么对产品经理有或没有价值。"
        )

    def batch_user_prompt(self, items):
        return json.dumps(
            [
                {
                    "item_id": item.get("item_id", ""),
                    "title": item.get("title", ""),
                    "summary": build_scoring_context(item)["summary_for_model"],
                    "summary_for_model": build_scoring_context(item)["summary_for_model"],
                    "source": item.get("source", ""),
                    "category": item.get("category", ""),
                    "source_kind": build_scoring_context(item)["source_kind"],
                    "scoring_mode": build_scoring_context(item)["scoring_mode"],
                    "context_quality": build_scoring_context(item)["context_quality"],
                    "summary_length": build_scoring_context(item)["summary_length"],
                    "domain": build_scoring_context(item)["domain"],
                }
                for item in items
            ],
            ensure_ascii=False,
        )


def save_day_scores(path: str | Path, scores: dict):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(scores, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def load_day_scores(path: str | Path):
    path = Path(path)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        return {}


def merge_scores_into_items(items, scores_by_id):
    merged = []
    for item in items:
        row = deepcopy(item)
        score = scores_by_id.get(item.get("item_id"), {})
        row["pm_label"] = score.get("pm_label")
        row["pm_score"] = score.get("pm_score")
        row["pm_reason"] = score.get("pm_reason")
        row["pm_signal_level"] = score.get("pm_signal_level")
        row["pm_decision_level"] = score.get("pm_decision_level")
        row["pm_transfer_level"] = score.get("pm_transfer_level")
        row["pm_evidence_level"] = score.get("pm_evidence_level")
        row["pm_constraint_level"] = score.get("pm_constraint_level")
        row["pm_model_profile"] = score.get("pm_model_profile")
        row["pm_model"] = score.get("pm_model")
        row["pm_scored_at"] = score.get("pm_scored_at")
        row["pm_score_status"] = score.get("pm_score_status")
        row["pm_error"] = score.get("pm_error")
        merged.append(row)
    return merged


def representative_items_for_scoring(day_payload, profile="auto"):
    items = day_payload.get("items", [])
    clusters = ((day_payload.get("clusters") or {}).get(profile) or [])
    rep_indices = {c["rep"] for c in clusters}
    consumed = {idx for c in clusters for idx in c.get("members", [])}
    selected = [items[idx] for idx in sorted(rep_indices)]
    selected.extend(items[idx] for idx in range(len(items)) if idx not in consumed)
    return selected


def _chunks(items, size):
    size = max(1, int(size or 1))
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _normalize_label(raw):
    if raw in VALID_PM_LABELS:
        return raw
    text = str(raw or "").strip().lower()
    if text in LABEL_ALIASES:
        return LABEL_ALIASES[text]
    if "必读" in text:
        return "必读"
    if "值得" in text:
        return "值得读"
    if "可选" in text:
        return "可选读"
    if any(token in text for token in ("略过", "跳过", "不读", "忽略")):
        return "略过"
    return None


def _normalize_score(raw, label):
    try:
        value = int(round(float(raw)))
    except (TypeError, ValueError):
        value = DEFAULT_PM_SCORES.get(label, 60)
    return max(0, min(100, value))


def _normalize_level(raw):
    text = str(raw or "").strip().lower()
    if text in LEVEL_ALIASES:
        return LEVEL_ALIASES[text]
    return None


def _extract_levels(payload):
    levels = {}
    for target_key, aliases in LEVEL_FIELDS.items():
        value = None
        for alias in aliases:
            if alias in payload:
                value = payload.get(alias)
                break
        value = _normalize_level(value)
        if value:
            levels[target_key] = value
    return levels


def _derive_label_from_levels(levels):
    signal = LEVEL_SCORE[levels["pm_signal_level"]]
    decision = LEVEL_SCORE[levels["pm_decision_level"]]
    transfer = LEVEL_SCORE[levels["pm_transfer_level"]]
    evidence = LEVEL_SCORE[levels["pm_evidence_level"]]
    constraint = LEVEL_SCORE[levels["pm_constraint_level"]]

    if signal == 0 and decision == 0:
        return "略过"
    if decision == 0 and evidence == 0:
        return "略过"
    if decision == 2 and signal == 2 and evidence == 2 and transfer == 2 and constraint >= 1:
        return "必读"
    if decision == 2 and signal >= 1 and evidence >= 1:
        return "值得读"
    if decision >= 1 and signal >= 1 and (transfer == 2 or constraint == 2):
        return "值得读"
    if decision >= 1 or signal >= 1 or transfer >= 1:
        return "可选读"
    return "略过"


def _normalize_reason(raw, item=None):
    text = str(raw or "").strip()
    if text:
        return text[:120]
    if item:
        category = item.get("category") or "该主题"
        return f"{category}里有一定产品判断价值"
    return "有一定产品判断价值"


def _conservative_title_only_score(item, profile):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "item_id": item["item_id"],
        "pm_label": "略过",
        "pm_score": DEFAULT_PM_SCORES["略过"],
        "pm_reason": "只有标题，缺少可靠摘要，先保守略过。",
        "pm_model_profile": profile.get("id") or profile.get("name"),
        "pm_model": profile.get("model"),
        "pm_scored_at": now,
        "pm_score_status": "done",
        "pm_error": None,
        "pm_signal_level": "low",
        "pm_decision_level": "low",
        "pm_transfer_level": "low",
        "pm_evidence_level": "low",
        "pm_constraint_level": "low",
    }


def _skip_short_summary_record(item, profile):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "item_id": item["item_id"],
        "pm_label": None,
        "pm_score": None,
        "pm_reason": f"摘要少于{MIN_SCORABLE_SUMMARY_CHARS}字，跳过评分。",
        "pm_model_profile": profile.get("id") or profile.get("name"),
        "pm_model": profile.get("model"),
        "pm_scored_at": now,
        "pm_score_status": "skipped",
        "pm_error": None,
        **{key: None for key in LEVEL_FIELDS},
    }


def _extract_json_from_text(text):
    text = str(text or "").strip()
    if not text:
        raise ValueError("empty model content")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    fenced_blocks = re.findall(r"```(?:json)?\s*(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)
    for block in fenced_blocks:
        block = block.strip()
        if not block:
            continue
        try:
            return json.loads(block)
        except json.JSONDecodeError:
            continue

    decoder = json.JSONDecoder()
    for idx, ch in enumerate(text):
        if ch not in "{[":
            continue
        try:
            payload, _ = decoder.raw_decode(text[idx:])
            return payload
        except json.JSONDecodeError:
            continue
    raise ValueError("no json payload found in model content")


def _extract_response_payload(response):
    if isinstance(response, (dict, list)):
        if isinstance(response, dict) and "choices" in response:
            try:
                content = response["choices"][0]["message"]["content"]
            except (KeyError, IndexError, TypeError) as exc:
                raise ValueError("invalid chat completion response shape") from exc
            if isinstance(content, list):
                content = "".join(
                    part.get("text", "")
                    for part in content
                    if isinstance(part, dict)
                )
            return _extract_json_from_text(content)
        return response
    return _extract_json_from_text(response)


def _normalize_single_result(payload, item):
    if isinstance(payload, list):
        if len(payload) != 1:
            raise ValueError("single score response unexpectedly returned multiple records")
        payload = payload[0]
    if not isinstance(payload, dict):
        raise ValueError("single score response must be a json object")

    label = _normalize_label(payload.get("pm_label") or payload.get("label") or payload.get("priority"))
    levels = _extract_levels(payload)
    if not label and len(levels) == len(LEVEL_FIELDS):
        label = _derive_label_from_levels(levels)
    if not label:
        raise ValueError("single score response missing valid pm_label")

    result = {
        "item_id": item["item_id"],
        "pm_label": label,
        "pm_score": _normalize_score(payload.get("pm_score") or payload.get("score"), label),
        "pm_reason": _normalize_reason(payload.get("pm_reason") or payload.get("reason"), item=item),
    }
    result.update(levels)
    return result


def _normalize_batch_results(payload, items):
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            payload = payload["results"]
        elif all(isinstance(value, dict) for value in payload.values()):
            payload = [{"item_id": key, **value} for key, value in payload.items()]
        else:
            payload = [payload]

    if not isinstance(payload, list):
        raise ValueError("batch score response must be a json array/object")

    items_by_id = {item["item_id"]: item for item in items}
    normalized = {}
    remaining_items = [item["item_id"] for item in items]

    for index, entry in enumerate(payload):
        if not isinstance(entry, dict):
            continue
        item_id = entry.get("item_id")
        if not item_id and index < len(remaining_items):
            item_id = remaining_items[index]
        if item_id not in items_by_id:
            continue
        try:
            normalized[item_id] = _normalize_single_result(entry, items_by_id[item_id])
        except ValueError:
            continue

    return normalized


class OpenAICompatibleScorer:
    def __init__(self, profile, transport=None):
        self.profile = profile
        self.transport = transport or self._default_transport

    def build_request_payload(self, item, builder=None):
        builder = builder or PromptBuilder()
        return {
            "model": self.profile["model"],
            "messages": [
                {"role": "system", "content": builder.system_prompt()},
                {"role": "user", "content": builder.user_prompt(item)},
            ],
            "response_format": {"type": "json_object"},
        }

    def build_batch_request_payload(self, items, builder=None):
        builder = builder or PromptBuilder()
        return {
            "model": self.profile["model"],
            "messages": [
                {"role": "system", "content": builder.batch_system_prompt()},
                {"role": "user", "content": builder.batch_user_prompt(items)},
            ],
        }

    def batch_size(self):
        try:
            size = int(self.profile.get("batch_size") or 1)
        except (TypeError, ValueError):
            size = 1
        return max(1, min(10, size))

    def _default_transport(self, payload):
        base_url = self.profile["base_url"].rstrip("/")
        ssl_context = ssl.create_default_context()
        req = urllib.request.Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.profile['api_key']}",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=60, context=ssl_context) as resp:
                body = json.loads(resp.read().decode("utf-8"))
            return _extract_response_payload(body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                _raise_transport_error(exc.code, body)
            except Exception as err:
                raise err from exc
        except Exception as exc:
            if self._should_fallback_to_curl(exc):
                return self._curl_transport(payload)
            if _is_transient_transport_error(exc):
                raise CooldownRetryError(str(exc)) from exc
            raise

    def _should_fallback_to_curl(self, exc):
        if not shutil.which("curl"):
            return False
        text = str(exc)
        return any(
            token in text
            for token in (
                "CERTIFICATE_VERIFY_FAILED",
                "self-signed certificate in certificate chain",
                "UNEXPECTED_EOF_WHILE_READING",
                "EOF occurred in violation of protocol",
            )
        )

    def _curl_transport(self, payload):
        base_url = self.profile["base_url"].rstrip("/")
        marker = "__HTTP_STATUS__:"
        connect_timeout = str(int(self.profile.get("connect_timeout") or 15))
        max_time = str(int(self.profile.get("request_timeout") or 75))
        proc = subprocess.run(
            [
                "curl",
                "-sS",
                "--connect-timeout",
                connect_timeout,
                "--max-time",
                max_time,
                "-w",
                f"\n{marker}%{{http_code}}",
                f"{base_url}/chat/completions",
                "-H",
                "Content-Type: application/json",
                "-H",
                f"Authorization: Bearer {self.profile['api_key']}",
                "--data",
                json.dumps(payload, ensure_ascii=False),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        combined = (proc.stdout or "") + (f"\n{proc.stderr}" if proc.stderr else "")
        if marker not in combined:
            if _is_transient_transport_error(text=combined):
                raise CooldownRetryError(combined.strip() or "temporary curl transport failed")
            raise RuntimeError(combined.strip() or "curl transport failed")
        body, _, status_text = combined.rpartition(marker)
        status = int((status_text or "0").splitlines()[0].strip() or "0")
        body = body.strip()
        if status == 0 or status >= 400:
            _raise_transport_error(status, body)
        parsed = json.loads(body)
        return _extract_response_payload(parsed)

    def score_one(self, item, builder=None):
        payload = self.build_request_payload(item, builder=builder)
        response = _extract_response_payload(self.transport(payload))
        normalized = _normalize_single_result(response, item)
        result = {
            "item_id": item["item_id"],
            "pm_label": normalized["pm_label"],
            "pm_score": normalized["pm_score"],
            "pm_reason": normalized["pm_reason"],
            "pm_model_profile": self.profile.get("id") or self.profile.get("name"),
            "pm_model": self.profile["model"],
            "pm_scored_at": datetime.now(timezone.utc).isoformat(),
            "pm_score_status": "done",
            "pm_error": None,
        }
        for key in LEVEL_FIELDS:
            result[key] = normalized.get(key)
        return result

    def score_batch(self, items, builder=None):
        payload = self.build_batch_request_payload(items, builder=builder)
        response = _extract_response_payload(self.transport(payload))
        normalized = _normalize_batch_results(response, items)
        if not normalized:
            raise ValueError("batch score response did not yield any valid records")
        now = datetime.now(timezone.utc).isoformat()
        results = []
        for item in items:
            row = normalized.get(item["item_id"])
            if not row:
                continue
            results.append(
                {
                    "item_id": item["item_id"],
                    "pm_label": row["pm_label"],
                    "pm_score": row["pm_score"],
                    "pm_reason": row["pm_reason"],
                    "pm_model_profile": self.profile.get("id") or self.profile.get("name"),
                    "pm_model": self.profile["model"],
                    "pm_scored_at": now,
                    "pm_score_status": "done",
                    "pm_error": None,
                    **{key: row.get(key) for key in LEVEL_FIELDS},
                }
            )
        return results

    def _error_result(self, item, exc):
        return {
            "item_id": item["item_id"],
            "pm_label": None,
            "pm_score": None,
            "pm_reason": None,
            "pm_model_profile": self.profile.get("id") or self.profile.get("name"),
            "pm_model": self.profile["model"],
            "pm_scored_at": datetime.now(timezone.utc).isoformat(),
            "pm_score_status": "error",
            "pm_error": str(exc)[:300],
            **{key: None for key in LEVEL_FIELDS},
        }

    def _score_one_safe(self, item, builder=None):
        try:
            return self.score_one(item, builder=builder)
        except CooldownRetryError:
            raise
        except Exception as exc:
            return self._error_result(item, exc)

    def _score_batch_with_fallback(self, items, builder=None):
        if not items:
            return []
        if len(items) == 1:
            return [self._score_one_safe(items[0], builder=builder)]
        try:
            batch_results = self.score_batch(items, builder=builder)
        except CooldownRetryError:
            raise
        except Exception:
            midpoint = max(1, len(items) // 2)
            left = self._score_batch_with_fallback(items[:midpoint], builder=builder)
            right = self._score_batch_with_fallback(items[midpoint:], builder=builder)
            return left + right

        found = {row["item_id"] for row in batch_results}
        results = list(batch_results)
        for item in items:
            if item["item_id"] not in found:
                results.append(self._score_one_safe(item, builder=builder))
        return results

    def score_many(self, items, builder=None):
        if self.batch_size() <= 1:
            return [self._score_one_safe(item, builder=builder) for item in items]

        results = []
        for batch in _chunks(items, self.batch_size()):
            results.extend(self._score_batch_with_fallback(batch, builder=builder))
        return results


def score_date(repo_dir, date_str, profile, scorer_factory=None, rebuild_day=None, force=False, settings=None):
    repo_dir = Path(repo_dir)
    day_path = repo_dir / "web" / "data" / "day" / f"{date_str}.json"
    if not day_path.exists():
        return 0
    day_payload = json.loads(day_path.read_text(encoding="utf-8"))
    items = representative_items_for_scoring(day_payload, profile="auto")
    if not items:
        return 0

    score_path = repo_dir / "data" / "ai-feed" / "scores" / f"{date_str}.json"
    existing = load_day_scores(score_path)
    representative_ids = {item["item_id"] for item in items}
    existing = {
        item_id: row
        for item_id, row in existing.items()
        if item_id in representative_ids
    }
    to_score = []
    local_done = 0
    local_skipped = 0
    for item in items:
        record = existing.get(item["item_id"])
        if (not force) and record and record.get("pm_score_status") in {"done", "skipped"}:
            continue
        context = build_scoring_context(item)
        if context["summary_length"] < MIN_SCORABLE_SUMMARY_CHARS:
            existing[item["item_id"]] = _skip_short_summary_record(item, profile)
            local_skipped += 1
            continue
        if context["scoring_mode"] == "title_only_article":
            existing[item["item_id"]] = _conservative_title_only_score(item, profile)
            local_done += 1
            continue
        existing[item["item_id"]] = {
            "item_id": item["item_id"],
            "pm_score_status": "pending",
            "pm_model_profile": profile.get("id") or profile.get("name"),
            "pm_model": profile.get("model"),
        }
        to_score.append(item)
    save_day_scores(score_path, existing)
    if not to_score:
        if local_done or local_skipped:
            rebuild_day = rebuild_day or (
                lambda target_date: subprocess.run(
                    [sys.executable, str(repo_dir / "scripts" / "build-web-data.py"), "--date", target_date],
                    check=True,
                )
            )
            rebuild_day(date_str)
        return local_done

    scorer_factory = scorer_factory or (lambda active_profile: OpenAICompatibleScorer(active_profile))
    settings = settings or {}
    rebuild_day = rebuild_day or (
        lambda target_date: subprocess.run(
            [sys.executable, str(repo_dir / "scripts" / "build-web-data.py"), "--date", target_date],
            check=True,
        )
    )
    total_done = 0
    raw_batch_size = settings.get("batch_size")
    if raw_batch_size is None:
        batch_size = getattr(scorer_factory(profile), "batch_size", lambda: 1)()
    else:
        try:
            batch_size = int(raw_batch_size)
        except (TypeError, ValueError):
            batch_size = getattr(scorer_factory(profile), "batch_size", lambda: 1)()
        batch_size = max(1, min(10, batch_size))
    raw_parallel_workers = settings.get("parallel_workers", 1)
    try:
        parallel_workers = int(raw_parallel_workers)
    except (TypeError, ValueError):
        parallel_workers = 1
    parallel_workers = max(1, min(5, parallel_workers))
    batches = list(_chunks(to_score, batch_size))

    def apply_batch_results(batch, results):
        nonlocal total_done
        found = {result["item_id"] for result in results}
        for result in results:
            existing[result["item_id"]] = result
            total_done += 1
        missing = [item for item in batch if item["item_id"] not in found]
        for item in missing:
            existing[item["item_id"]] = {
                **existing.get(item["item_id"], {"item_id": item["item_id"]}),
                "pm_score_status": "error",
                "pm_error": "missing result from scorer",
                "pm_model_profile": profile.get("id") or profile.get("name"),
                "pm_model": profile.get("model"),
            }
        save_day_scores(score_path, existing)

    if parallel_workers <= 1 or len(batches) <= 1:
        scorer = scorer_factory(profile)
        for batch in batches:
            try:
                results = scorer.score_many(batch)
                apply_batch_results(batch, results)
            except CooldownRetryError:
                save_day_scores(score_path, existing)
                raise
            except Exception as exc:
                error_text = str(exc)[:300]
                for item in batch:
                    existing[item["item_id"]] = {
                        **existing.get(item["item_id"], {"item_id": item["item_id"]}),
                        "pm_score_status": "error",
                        "pm_error": error_text,
                        "pm_model_profile": profile.get("id") or profile.get("name"),
                        "pm_model": profile.get("model"),
                    }
                save_day_scores(score_path, existing)
    else:
        cooldown_exc = None
        worker_count = min(parallel_workers, len(batches))

        def run_batch(batch):
            scorer = scorer_factory(profile)
            return scorer.score_many(batch)

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
                            "pm_score_status": "error",
                            "pm_error": error_text,
                            "pm_model_profile": profile.get("id") or profile.get("name"),
                            "pm_model": profile.get("model"),
                        }
                    save_day_scores(score_path, existing)
        if cooldown_exc:
            save_day_scores(score_path, existing)
            raise cooldown_exc
    rebuild_day(date_str)
    return total_done + local_done


def score_dates(repo_dir, dates, profile, scorer_factory=None, rebuild_day=None, force=False, settings=None):
    total = 0
    for date_str in dates:
        total += score_date(repo_dir, date_str, profile, scorer_factory=scorer_factory, rebuild_day=rebuild_day, force=force, settings=settings)
    return total
