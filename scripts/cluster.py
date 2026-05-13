"""Cluster near-duplicate items within a single day, per category.

Design goals (in priority order):
1.  **Precision over recall.** A wrong merge is worse than a missed one — the
    user then sees two real stories as one and can't tell.
2.  **Deterministic.** Same input → same clusters. Rebuildable.
3.  **No heavy deps.** Stdlib only. CJK-friendly (bigram Jaccard over characters).
4.  **Explainable.** Every merge is driven by (a) category match, (b) title
    bigram similarity, (c) discriminator compatibility, (d) entity overlap for
    weak matches. These can be logged.

Algorithm:
    For items within the same (day, category):
      - shingles(i) = set of char bigrams of normalized title
      - discs(i)   = set of version/percent/money tokens from title+summary
                     (e.g. "4.7", "25%", "$2b")
      - ents(i)    = set of proper-noun / model tokens (Claude, GPT-4, OpenAI…)
      - sim(i,j)   = |shingles(i) ∩ shingles(j)| / |shingles(i) ∪ shingles(j)|

      Merge i and j iff:
        (A) category(i) == category(j)                       — hard wall
        (B) discs(i) and discs(j) non-empty  ⇒ discs(i) == discs(j)
                                                               — block "4.7 vs 4.6"
        (C) sim(i,j) >= TAU_STRONG                           — trust title alone
        or  TAU_WEAK <= sim(i,j) < TAU_STRONG
                and |ents(i) ∩ ents(j)| >= 1                 — require shared entity

    Union-find over satisfying pairs, then split any cluster > MAX_CLUSTER by
    removing the weakest edges until components fit.

Representative selection within a cluster:
    Longest summary wins (most informative). Ties broken by earliest segment.
"""
from __future__ import annotations
import re
from collections import defaultdict
from typing import Iterable

# ───────────────────────── Tunables ─────────────────────────
TAU_STRONG   = 0.72
TAU_WEAK     = 0.50
MAX_CLUSTER  = 8

# Aggressive profile — used when the user picks "Aggressive" in UI (build both).
TAU_STRONG_AGG = 0.60
TAU_WEAK_AGG   = 0.38

# ─────────────────────── Feature extraction ──────────────────
_NORM_STRIP = re.compile(r"[\s\u3000\-_·，,。.！!？?：:；;（）()【】\[\]\"“”'‘’、/\\|]")
_DISC_RE = re.compile(
    r"(?:"
    r"\d+\.\d+(?:\.\d+)*"         # version: 4.7, 2.1.3
    r"|\d+%"                      # percent
    r"|\$\d+(?:\.\d+)?[mbkMBK]?"  # money: $2b, $1.5m
    r"|\d{2,4}[万亿]"             # chinese magnitudes: 50亿, 300万
    r"|Q[1-4]\s?\d{2,4}"          # quarters
    r")"
)
# Proper-noun-ish tokens common in AI/tech news. Cheap but effective for this
# corpus. Extendable via config.
_ENT_RE = re.compile(
    r"("
    r"Claude|Anthropic|OpenAI|GPT-?\d(?:\.\d+)?|ChatGPT|Codex|Sora"
    r"|Gemini|Google|DeepMind|Nano\s?Banana"
    r"|Meta|Llama\s?\d+(?:\.\d+)?|PyTorch"
    r"|Mistral|Cohere|xAI|Grok|Groq"
    r"|NVIDIA|Nvidia|CUDA|Blackwell|H100|H200|B200"
    r"|Microsoft|Azure|Copilot|GitHub|Copilot"
    r"|Amazon|AWS|Bedrock"
    r"|Apple|iPhone|Vision\s?Pro"
    r"|Cloudflare|Vercel|Supabase|Replit"
    r"|DeepSeek|Qwen|Kimi|Yi|Moonshot|Baichuan|智谱|通义|文心|豆包|混元"
    r"|Tesla|ByteDance|Tencent|Alibaba|Baidu"
    r"|Perplexity|Cursor|Windsurf|Figma|Notion"
    r"|HuggingFace|Hugging\s?Face|LangChain|LlamaIndex"
    r"|Sam\s?Altman|Elon\s?Musk|Dario\s?Amodei|Jensen\s?Huang"
    r"|Stripe|Airbnb|Uber|Palantir|Databricks|Snowflake"
    r")",
    re.IGNORECASE,
)

def normalize_title(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = _NORM_STRIP.sub("", s)
    return s

def shingles(s: str, n: int = 2) -> frozenset:
    s = normalize_title(s)
    if len(s) < n:
        return frozenset([s]) if s else frozenset()
    return frozenset(s[i : i + n] for i in range(len(s) - n + 1))

def discriminators(text: str) -> frozenset:
    if not text:
        return frozenset()
    return frozenset(m.group(0).lower().replace(" ", "") for m in _DISC_RE.finditer(text))

def entities(text: str) -> frozenset:
    if not text:
        return frozenset()
    return frozenset(
        re.sub(r"\s+", "", m.group(0)).lower() for m in _ENT_RE.finditer(text)
    )

def jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)

# ─────────────────────── Main entry point ────────────────────
def cluster_items(items: list, profile: str = "auto") -> list:
    """Return list of clusters. Each cluster is a list of item indices into `items`.
    A singleton is still returned as a 1-element cluster.

    profile: "auto" | "aggressive" | "off"
    """
    if profile == "off" or not items:
        return [[i] for i in range(len(items))]

    if profile == "aggressive":
        t_strong, t_weak = TAU_STRONG_AGG, TAU_WEAK_AGG
    else:
        t_strong, t_weak = TAU_STRONG, TAU_WEAK

    feats = []
    for it in items:
        title = it.get("title") or ""
        summary = it.get("summary") or ""
        blob = f"{title} {summary}"
        feats.append({
            "shingles": shingles(title),
            "discs": discriminators(blob),
            "ents": entities(blob),
            "cat": it.get("category", ""),
        })

    n = len(items)
    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Group by category to keep O(N²) tight (inside each bucket).
    by_cat = defaultdict(list)
    for i, f in enumerate(feats):
        by_cat[f["cat"]].append(i)

    edges = []   # (sim, i, j) — keep for potential cluster splitting
    for cat, idxs in by_cat.items():
        for ii in range(len(idxs)):
            i = idxs[ii]
            fi = feats[i]
            if not fi["shingles"]:
                continue
            for jj in range(ii + 1, len(idxs)):
                j = idxs[jj]
                fj = feats[j]
                if not fj["shingles"]:
                    continue
                # (B) discriminator block
                if fi["discs"] and fj["discs"] and fi["discs"] != fj["discs"]:
                    # one has {"4.7"}, other has {"4.6"} — different stories
                    # BUT if the intersection is non-empty we allow it (mixed topics)
                    if not (fi["discs"] & fj["discs"]):
                        continue
                sim = jaccard(fi["shingles"], fj["shingles"])
                if sim < t_weak:
                    continue
                if sim < t_strong:
                    # (D) weak tier — require entity overlap
                    if not (fi["ents"] & fj["ents"]):
                        continue
                union(i, j)
                edges.append((sim, i, j))

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)

    result = list(groups.values())

    # Enforce MAX_CLUSTER — split the largest clusters by dropping the weakest
    # edges inside them until components fit.
    result = _enforce_cap(result, edges, cap=MAX_CLUSTER)

    # Stable ordering: by min original index
    for g in result:
        g.sort()
    result.sort(key=lambda g: g[0])
    return result

def _enforce_cap(groups, edges, cap):
    big = [g for g in groups if len(g) > cap]
    if not big:
        return groups
    small = [g for g in groups if len(g) <= cap]
    member_to_group = {}
    for gi, g in enumerate(big):
        for m in g:
            member_to_group[m] = gi
    # Edges within big clusters, sorted weakest-first
    internal = sorted(
        [(s, i, j) for s, i, j in edges if member_to_group.get(i) is not None and member_to_group.get(i) == member_to_group.get(j)],
        key=lambda e: e[0],
    )
    # Rebuild union-find per big cluster, adding edges strongest-first, but
    # forbid unions that would exceed cap.
    out = []
    for gi, g in enumerate(big):
        edges_g = [e for e in reversed(internal) if member_to_group[e[1]] == gi]
        parent = {m: m for m in g}
        size = {m: 1 for m in g}
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        for _s, i, j in edges_g:
            ri, rj = find(i), find(j)
            if ri == rj:
                continue
            if size[ri] + size[rj] > cap:
                continue
            parent[ri] = rj
            size[rj] += size[ri]
        sub = defaultdict(list)
        for m in g:
            sub[find(m)].append(m)
        out.extend(sub.values())
    return small + out

# ─────────────────────── Cluster summary ─────────────────────
def summarize_cluster(items: list, idxs: list[int]) -> dict:
    """Build the compact cluster descriptor written into day JSON."""
    members = [items[i] for i in idxs]
    # Representative: longest summary, then earliest segment.
    def keyfn(it):
        return (-len(it.get("summary") or ""), it.get("segment") or "zz")
    rep = min(members, key=keyfn)
    rep_idx = idxs[members.index(rep)]
    sources = {}
    for it in members:
        s = it.get("source") or "?"
        sources[s] = sources.get(s, 0) + 1
    return {
        "rep_id": rep.get("item_id"),
        "rep_index": rep_idx,
        "member_ids": [it.get("item_id") for it in members],
        "member_indices": idxs,
        "size": len(members),
        "sources": sources,
    }
