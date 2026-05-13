#!/usr/bin/env python3
"""Runtime config loader/writer for local UI-managed settings."""

from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path

try:
    from scripts.rss_labels import CANONICAL_CATEGORY_EMOJIS, normalize_category_label, normalize_rss_config
except ModuleNotFoundError:  # pragma: no cover - script import fallback
    from rss_labels import CANONICAL_CATEGORY_EMOJIS, normalize_category_label, normalize_rss_config

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - environment-dependent fallback
    yaml = None


CONFIG_SECTION_DEFAULTS = {
    "scoring": {
        "enabled": False,
        "active_profile": None,
        "profiles": [],
        "parallel_workers": 1,
        "rss": {"enabled": False, "max_items": 50},
    },
    "tagging": {
        "enabled": False,
        "active_profile": None,
        "profiles": [],
        "batch_size": 5,
        "parallel_workers": 1,
        "max_pending_per_run": 50,
        "allow_inherit_from_cluster": True,
    },
    "daily_digest": {
        "enabled": False,
        "active_profile": None,
        "profiles": [],
        "parallel_workers": 1,
        "schedule": {"time": "08:30"},
        "outputs": {"web": True, "obsidian": True},
    },
}


def _deep_merge(base, override):
    if not isinstance(base, dict) or not isinstance(override, dict):
        return deepcopy(override)
    merged = deepcopy(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _parse_scalar(value):
    value = re.sub(r"\s+#.*$", "", value).strip()
    if value in {"true", "false"}:
        return value == "true"
    if value in {"null", "None", "~"}:
        return None
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(part) for part in inner.split(",")]
    try:
        return int(value)
    except ValueError:
        pass
    return value


def _indent_of(line):
    return len(line) - len(line.lstrip(" "))


def _clean_yaml_lines(raw):
    lines = []
    for raw_line in raw.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        lines.append(raw_line.rstrip("\n"))
    return lines


def _parse_yaml_subset(lines, start=0, indent=0):
    if start >= len(lines):
        return {}, start
    is_list = lines[start].lstrip().startswith("- ") and _indent_of(lines[start]) == indent
    if is_list:
        out = []
        idx = start
        while idx < len(lines):
            line = lines[idx]
            if _indent_of(line) != indent or not line.lstrip().startswith("- "):
                break
            content = line.lstrip()[2:].strip()
            idx += 1
            if not content:
                value, idx = _parse_yaml_subset(lines, idx, indent + 2)
                out.append(value)
                continue
            if ":" in content:
                key, _, rest = content.partition(":")
                item = {key.strip(): _parse_scalar(rest) if rest.strip() else {}}
                if not rest.strip() and idx < len(lines) and _indent_of(lines[idx]) > indent:
                    value, idx = _parse_yaml_subset(lines, idx, indent + 2)
                    item[key.strip()] = value
                while idx < len(lines):
                    next_line = lines[idx]
                    next_indent = _indent_of(next_line)
                    if next_indent <= indent:
                        break
                    if next_indent != indent + 2 or next_line.lstrip().startswith("- "):
                        break
                    n_key, _, n_rest = next_line.strip().partition(":")
                    idx += 1
                    if n_rest.strip():
                        item[n_key.strip()] = _parse_scalar(n_rest)
                    else:
                        value, idx = _parse_yaml_subset(lines, idx, indent + 4)
                        item[n_key.strip()] = value
                out.append(item)
            else:
                out.append(_parse_scalar(content))
        return out, idx

    out = {}
    idx = start
    while idx < len(lines):
        line = lines[idx]
        current_indent = _indent_of(line)
        if current_indent < indent:
            break
        if current_indent > indent:
            idx += 1
            continue
        key, _, rest = line.strip().partition(":")
        idx += 1
        if rest.strip():
            out[key.strip()] = _parse_scalar(rest)
            continue
        if idx < len(lines) and _indent_of(lines[idx]) > indent:
            value, idx = _parse_yaml_subset(lines, idx, indent + 2)
            out[key.strip()] = value
        else:
            out[key.strip()] = {}
    return out, idx


def _load_project_yaml_without_pyyaml(raw):
    lines = _clean_yaml_lines(raw)
    payload, _ = _parse_yaml_subset(lines, start=0, indent=0)
    return payload if isinstance(payload, dict) else {}


class RuntimeConfigStore:
    def __init__(self, base_path: str | Path, local_path: str | Path | None = None):
        self.base_path = Path(base_path)
        self.local_path = Path(local_path) if local_path else self.base_path.with_name("config.local.json")

    def _apply_defaults(self, data):
        data = deepcopy(data or {})
        for section, defaults in CONFIG_SECTION_DEFAULTS.items():
            data.setdefault(section, {})
            data[section] = _deep_merge(defaults, data.get(section) or {})
            data[section].setdefault("profiles", [])
            if not data[section].get("active_profile") and data[section].get("profiles"):
                data[section]["active_profile"] = data[section]["profiles"][0].get("id")
        return data

    def _load_yaml(self):
        raw = self.base_path.read_text(encoding="utf-8")
        if yaml is not None:
            data = yaml.safe_load(raw) or {}
        else:
            data = _load_project_yaml_without_pyyaml(raw)
        return self._apply_defaults(data)

    def _load_local(self):
        if not self.local_path.exists():
            return {}
        return json.loads(self.local_path.read_text(encoding="utf-8") or "{}")

    def _normalize_runtime_data(self, data):
        normalized = deepcopy(data or {})

        categories = []
        seen_labels = set()
        for category in normalized.get("categories", []) or []:
            row = deepcopy(category or {})
            label = normalize_category_label(row.get("label"))
            if not label or label in seen_labels:
                continue
            row["label"] = label
            if label in CANONICAL_CATEGORY_EMOJIS and not str(row.get("emoji") or "").strip():
                row["emoji"] = CANONICAL_CATEGORY_EMOJIS[label]
            categories.append(row)
            seen_labels.add(label)
        if categories:
            categories.sort(key=lambda item: (int(item.get("order") or 0), str(item.get("label") or "")))
            normalized["categories"] = categories

        sources = []
        for source in normalized.get("sources", []) or []:
            row = deepcopy(source or {})
            if row.get("type") == "rss":
                row["config"] = normalize_rss_config(row.get("config") or {})
            sources.append(row)
        if sources:
            normalized["sources"] = sources

        return normalized

    def load(self):
        base = self._load_yaml()
        merged = _deep_merge(base, self._load_local())
        merged = self._apply_defaults(merged)
        for section in CONFIG_SECTION_DEFAULTS:
            if not merged[section].get("profiles"):
                merged[section]["profiles"] = deepcopy(base.get(section, {}).get("profiles", []))
            if not merged[section].get("active_profile") and merged[section].get("profiles"):
                merged[section]["active_profile"] = merged[section]["profiles"][0].get("id")
        return self._normalize_runtime_data(merged)

    def save_local(self, payload):
        self.local_path.parent.mkdir(parents=True, exist_ok=True)
        self.local_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return self.load()

    def _current_local(self):
        return self._apply_defaults(self._load_local())

    def update_section(self, kind, updates):
        if kind not in CONFIG_SECTION_DEFAULTS:
            raise KeyError(kind)
        local = self._current_local()
        section = local[kind]
        for key, value in (updates or {}).items():
            if key == "profiles":
                continue
            if isinstance(value, dict) and isinstance(section.get(key), dict):
                section[key] = _deep_merge(section.get(key, {}), value)
            else:
                section[key] = deepcopy(value)
        return self.save_local(local)

    def update_scoring_settings(self, updates):
        return self.update_section("scoring", updates)

    def upsert_profile(self, profile, activate=False, kind="scoring"):
        if kind not in CONFIG_SECTION_DEFAULTS:
            raise KeyError(kind)
        local = self._current_local()
        section = local[kind]
        profiles = section.setdefault("profiles", [])
        incoming = deepcopy(profile)
        existing_idx = next((i for i, p in enumerate(profiles) if p.get("id") == incoming.get("id")), None)
        if existing_idx is None:
            profiles.append(incoming)
        else:
            profiles[existing_idx] = incoming
        if activate or not section.get("active_profile"):
            section["active_profile"] = incoming.get("id")
        return self.save_local(local)

    def delete_profile(self, profile_id, kind="scoring"):
        if kind not in CONFIG_SECTION_DEFAULTS:
            raise KeyError(kind)
        local = self._current_local()
        section = local[kind]
        section["profiles"] = [p for p in section.get("profiles", []) if p.get("id") != profile_id]
        if section.get("active_profile") == profile_id:
            if section["profiles"]:
                section["active_profile"] = section["profiles"][0].get("id")
            else:
                section["active_profile"] = None
        return self.save_local(local)

    def set_active_profile(self, profile_id, kind="scoring"):
        return self.update_section(kind, {"active_profile": profile_id})

    def active_profile(self, kind="scoring"):
        cfg = self.load()
        section = cfg.get(kind, {})
        active = section.get("active_profile")
        for profile in section.get("profiles", []):
            if profile.get("id") == active:
                return profile
        return None

    def _save_top_level_key(self, key, value):
        local = self._current_local()
        local[key] = deepcopy(value)
        return self.save_local(local)

    def upsert_source(self, source):
        merged = self.load()
        sources = deepcopy(merged.get("sources", []) or [])
        incoming = deepcopy(source or {})
        source_id = str(incoming.get("id") or "").strip()
        if not source_id:
            raise ValueError("source.id is required")
        existing_idx = next((idx for idx, item in enumerate(sources) if item.get("id") == source_id), None)
        if existing_idx is None:
            sources.append(incoming)
        else:
            sources[existing_idx] = incoming
        return self._save_top_level_key("sources", sources)

    def delete_source(self, source_id):
        merged = self.load()
        sources = [deepcopy(item) for item in (merged.get("sources", []) or []) if item.get("id") != source_id]
        return self._save_top_level_key("sources", sources)

    def upsert_category(self, category):
        merged = self.load()
        categories = deepcopy(merged.get("categories", []) or [])
        incoming = deepcopy(category or {})
        label = str(incoming.get("label") or "").strip()
        if not label:
            raise ValueError("category.label is required")
        if not incoming.get("emoji"):
            incoming["emoji"] = label.split(" ", 1)[0] if label else "📦"
        if incoming.get("order") is None:
            max_order = max((int(item.get("order") or 0) for item in categories), default=0)
            incoming["order"] = max_order + 1
        existing_idx = next((idx for idx, item in enumerate(categories) if item.get("label") == label), None)
        if existing_idx is None:
            categories.append(incoming)
        else:
            current = categories[existing_idx]
            categories[existing_idx] = {
                **current,
                **incoming,
                "order": incoming.get("order", current.get("order")),
            }
        categories.sort(key=lambda item: (int(item.get("order") or 0), str(item.get("label") or "")))
        return self._save_top_level_key("categories", categories)

    def delete_category(self, label):
        merged = self.load()
        categories = [deepcopy(item) for item in (merged.get("categories", []) or []) if item.get("label") != label]
        return self._save_top_level_key("categories", categories)
