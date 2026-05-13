#!/usr/bin/env python3
"""Canonical labels for RSS source/category display."""

from __future__ import annotations

from copy import deepcopy
from urllib.parse import urlparse


RSS_CATEGORY = "RSS源"
WECHAT_CATEGORY = "公众号"
DEFAULT_CATEGORY = "📦 其他"

RSS_CATEGORY_ALIASES = {
    "ak rss源": RSS_CATEGORY,
    "AK rss源": RSS_CATEGORY,
    "ak RSS源": RSS_CATEGORY,
    "rss源": RSS_CATEGORY,
    "公众号分类": WECHAT_CATEGORY,
}

CANONICAL_CATEGORY_EMOJIS = {
    RSS_CATEGORY: "🧩",
    WECHAT_CATEGORY: "📬",
}


def is_local_feed_url(url):
    parsed = urlparse(str(url or "").strip())
    host = (parsed.hostname or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "0.0.0.0"}


def normalize_rss_category(category, *, feed_url=""):
    raw = str(category or "").strip()
    if raw in RSS_CATEGORY_ALIASES:
        return RSS_CATEGORY_ALIASES[raw]
    if raw:
        return raw
    if feed_url:
        return WECHAT_CATEGORY if is_local_feed_url(feed_url) else RSS_CATEGORY
    return DEFAULT_CATEGORY


def normalize_category_label(label):
    return RSS_CATEGORY_ALIASES.get(str(label or "").strip(), str(label or "").strip())


def rss_source_label(feed_url):
    return WECHAT_CATEGORY if is_local_feed_url(feed_url) else RSS_CATEGORY


def normalize_rss_config(config):
    normalized = deepcopy(config or {})
    feed_url = str(normalized.get("feed_url") or "").strip()
    if not feed_url:
        return normalized
    normalized["category"] = normalize_rss_category(normalized.get("category"), feed_url=feed_url)
    if not str(normalized.get("source_label") or "").strip():
        normalized["source_label"] = rss_source_label(feed_url)
    return normalized
