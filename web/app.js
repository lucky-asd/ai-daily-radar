// AI Feed · Dispatch — timeline reader.
//
// Loads data/index.json once, then lazy-loads data/day/<date>.json on demand.
// Renders a timeline of days (newest first), with items grouped by category.
// Settings persist in localStorage. No build step.

// ─────────────────────────── State ───────────────────────────
const DEFAULT_SETTINGS = {
  theme: "auto",       // auto | light | dark
  density: "cozy",     // compact | cozy | roomy
  group: "category",   // category | source | flat
  cluster: "auto",     // auto | aggressive | off
  summary: "on",       // on | off
  digestReadMode: "full", // full | brief
};
const SCORE_LABELS = ["全部", "必读", "值得读", "可选读", "略过", "未评分"];
const LEVEL_FILTERS = ["全部", "low", "medium", "high"];
const ITEM_STATE_FILTERS = [
  { key: "starred", label: "收藏" },
  { key: "read_later", label: "稍后读" },
  { key: "read", label: "已读" },
  { key: "unread", label: "未读" },
];
const SCORE_DIMENSIONS = [
  { key: "signal", label: "Signal", field: "pm_signal_level", mount: "#signal-filters", clear: "#clear-signal-filters" },
  { key: "decision", label: "Decision", field: "pm_decision_level", mount: "#decision-filters", clear: "#clear-decision-filters" },
  { key: "transfer", label: "Transfer", field: "pm_transfer_level", mount: "#transfer-filters", clear: "#clear-transfer-filters" },
  { key: "evidence", label: "Evidence", field: "pm_evidence_level", mount: "#evidence-filters", clear: "#clear-evidence-filters" },
  { key: "constraint", label: "Constraint", field: "pm_constraint_level", mount: "#constraint-filters", clear: "#clear-constraint-filters" },
];

const LS_KEY = "ai-feed:settings:v1";
const LS_OPEN = "ai-feed:sections:v1";
const LS_SETTINGS_TAB = "ai-feed:settings-tab:v1";
const LS_WORKBENCH = "ai-feed:workbench-open:v3";
const FEED_GROUP_INITIAL_COUNT = 18;
const FEED_GROUP_BATCH_COUNT = 18;
const FEED_FLAT_INITIAL_COUNT = 120;
const FEED_FLAT_BATCH_COUNT = 120;
const TIMELINE_DAY_BATCH_COUNT = 1;
const AUTO_ITEM_LOAD_ROOT_MARGIN = "900px 0px 900px 0px";
const AUTO_DAY_LOAD_ROOT_MARGIN = "700px 0px 900px 0px";

const LOCAL_DEVELOPMENT_HOSTS = new Set(["", "localhost", "127.0.0.1", "0.0.0.0", "::1"]);
const STATIC_PREVIEW_PORTS = new Set(["48917"]);
const IS_STATIC_SITE = (() => {
  const host = window.location.hostname || "";
  if (window.location.protocol === "file:") return true;
  if (STATIC_PREVIEW_PORTS.has(window.location.port || "")) return true;
  if (LOCAL_DEVELOPMENT_HOSTS.has(host)) return false;
  if (/^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.)/.test(host)) return false;
  return true;
})();
const STATIC_SITE_SOURCE_STATUS = {
  ok: true,
  problem: null,
  auth: { ok: true, token_status: "static" },
  hourly_state: {},
  latest_issue: null,
  auth_session: null,
  recommended_action: "none",
  mode: "static",
};
const STATIC_SITE_RUNTIME_STATUS = {
  running: false,
  paused: false,
  queued_dates: [],
  pending_jobs: [],
  backfill_jobs: [],
  current_job: null,
  current_jobs: [],
  recent_jobs: [],
  config: { enabled: false, active_profile: null },
};

function backendDisabledInStaticMode() {
  if (IS_STATIC_SITE) return true;
  return false;
}

const state = {
  index: null,
  loadedDays: new Map(),      // date -> { cards, items }
  timelineCount: 1,           // how many days rendered initially in continuous mode
  timelineWindowStartIndex: null,
  timelineWindowEndIndex: null,
  timelineAutoLoading: false,
  timelineAutoLoadDisabledUntil: 0,
  itemRenderCounts: new Map(),
  selectedDate: null,
  selectedDigestDate: null,
  activeDigestTab: "local",
  activeView: "feed",
  activeCategories: new Set(),
  activeSources: new Set(),
  activeEntityTags: new Set(),
  activeTopicTags: new Set(),
  activeScoreLabels: new Set(),
  activeItemStates: new Set(),
  dayQuickFilters: { date: null, source: "", score: "" },
  activeLevels: {
    signal: new Set(),
    decision: new Set(),
    transfer: new Set(),
    evidence: new Set(),
    constraint: new Set(),
  },
  userStates: new Map(),
  search: "",
  cacheToken: Date.now(),
  refreshing: false,
  digestRefreshing: false,
  digestRenderSeq: 0,
  digestIndex: { dates: [] },
  digestCache: new Map(),
  sourceConfig: { sources: [], categories: [] },
  selectedSourceIds: new Set(),
  sourceFilterSearch: "",
  sourceFilterCategory: "",
  sourceFilterStatus: "all",
  scoringConfig: { enabled: false, active_profile: null, profiles: [], parallel_workers: 1, rss: { enabled: true, max_items: 80 } },
  taggingConfig: { enabled: false, active_profile: null, profiles: [], parallel_workers: 1, max_pending_per_run: 50, allow_inherit_from_cluster: true },
  digestConfig: { enabled: false, active_profile: null, profiles: [], parallel_workers: 1, schedule: { time: "08:30" }, outputs: { web: true, obsidian: true } },
  scoringStatus: { running: false, paused: false, queued_dates: [], pending_jobs: [], backfill_jobs: [], current_job: null, current_jobs: [], recent_jobs: [], config: { enabled: false, active_profile: null } },
  taggingStatus: { running: false, paused: false, queued_dates: [], pending_jobs: [], backfill_jobs: [], current_job: null, recent_jobs: [], config: { enabled: false, active_profile: null } },
  digestStatus: { running: false, paused: false, queued_dates: [], pending_jobs: [], current_job: null, current_jobs: [], recent_jobs: [], config: { enabled: false, active_profile: null, parallel_workers: 1, schedule: { time: "08:30" }, outputs: { web: true, obsidian: true } } },
  sourceStatus: { ok: true, problem: null, auth: { ok: true, token_status: "unknown" }, hourly_state: {}, latest_issue: null, auth_session: null, recommended_action: "none" },
  settingsTab: loadSettingsTab(),
  runtimeJobDetails: new Map(),
  taggingJobDetails: new Map(),
  selectedRuntimeJobId: null,
  selectedTaggingJobId: null,
  selectedDigestJobId: null,
  runtimeDetailTab: "done",
  taggingDetailTab: "done",
  settings: loadSettings(),
  sections: loadSections(),   // { EDITIONS: true, ... }
  workbenchExpanded: loadWorkbenchExpanded(),
  privateBundle: null,
};

// Some upstream items come in malformed with markdown in the title,
// occasionally with the closing `)` truncated. Strip defensively.
function cleanTitle(t) {
  if (!t) return "(无标题)";
  let s = String(t);
  s = s.replace(/\*\*(.+?)\*\*/g, "$1");
  // Well-formed [text](url)
  s = s.replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, "$1");
  // Truncated [text](url...  (no closing paren)
  s = s.replace(/\[([^\]]+)\]\(https?:\/\/[^)\s]*/g, "$1");
  // Any leftover stray brackets
  s = s.replace(/^\s*\[|\]\s*$/g, "");
  return s.trim() || "(无标题)";
}

// Upstream occasionally shoves the whole English tweet body into `author`.
// Don't display something that obviously isn't a name.
function sanitizeAuthor(a) {
  if (!a) return null;
  const s = String(a).trim();
  if (s.length > 40) return null;
  if (s.split(/\s+/).length > 5) return null;
  return s;
}

// ─────────────────────────── Helpers ─────────────────────────
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => [...r.querySelectorAll(s)];
function h(tag, attrs = {}, children = []) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k === "html") el.innerHTML = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2).toLowerCase(), v);
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const c of [].concat(children)) {
    if (c == null || c === false) continue;
    el.appendChild(typeof c === "string" || typeof c === "number" ? document.createTextNode(String(c)) : c);
  }
  return el;
}

function decodeBase64Bytes(value) {
  const raw = atob(String(value || ""));
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i += 1) bytes[i] = raw.charCodeAt(i);
  return bytes;
}

async function decryptPrivateBundle(encrypted, password) {
  if (!encrypted || encrypted.version !== 1 || encrypted.algorithm !== "AES-GCM") {
    throw new Error("私有数据包格式不支持");
  }
  if (!window.crypto?.subtle) throw new Error("当前浏览器不支持本地解密");
  const kdf = encrypted.kdf || {};
  const salt = decodeBase64Bytes(kdf.salt);
  const iv = decodeBase64Bytes(encrypted.iv);
  const ciphertext = decodeBase64Bytes(encrypted.ciphertext);
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(password),
    "PBKDF2",
    false,
    ["deriveKey"],
  );
  const key = await crypto.subtle.deriveKey(
    { name: "PBKDF2", salt, iterations: Number(kdf.iterations || 210000), hash: kdf.hash || "SHA-256" },
    keyMaterial,
    { name: "AES-GCM", length: 256 },
    false,
    ["decrypt"],
  );
  const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, key, ciphertext);
  const text = await decodePrivatePlaintext(plaintext, encrypted.compression);
  return JSON.parse(text);
}

async function decodePrivatePlaintext(buffer, compression) {
  if (!compression) return new TextDecoder().decode(buffer);
  if (compression !== "gzip") throw new Error(`私有数据包压缩格式不支持：${compression}`);
  if (typeof DecompressionStream === "undefined") {
    throw new Error("当前浏览器不支持解压私有数据包，请升级 Chrome 或 Edge");
  }
  const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Response(stream).text();
}

function loadSettings() {
  try { return { ...DEFAULT_SETTINGS, ...JSON.parse(localStorage.getItem(LS_KEY) || "{}") }; }
  catch { return { ...DEFAULT_SETTINGS }; }
}
function saveSettings() { localStorage.setItem(LS_KEY, JSON.stringify(state.settings)); }
function loadSections() {
  try { return JSON.parse(localStorage.getItem(LS_OPEN) || "null") || { EDITIONS: true, CATEGORIES: true, SOURCES: true, "WORTH READING": true, "ENTITY TAGS": true, "TOPIC TAGS": true, "MY LISTS": true }; }
  catch { return { EDITIONS: true, CATEGORIES: true, SOURCES: true, "WORTH READING": true, "ENTITY TAGS": true, "TOPIC TAGS": true, "MY LISTS": true }; }
}
function saveSections() { localStorage.setItem(LS_OPEN, JSON.stringify(state.sections)); }
function loadSettingsTab() {
  try {
    const value = localStorage.getItem(LS_SETTINGS_TAB) || "reading";
    return ["reading", "sources", "scoring", "tagging", "digest"].includes(value) ? value : "reading";
  } catch {
    return "reading";
  }
}
function saveSettingsTab() { localStorage.setItem(LS_SETTINGS_TAB, state.settingsTab); }
function loadWorkbenchExpanded() {
  try {
    return localStorage.getItem(LS_WORKBENCH) === "1";
  } catch {
    return false;
  }
}
function saveWorkbenchExpanded() {
  localStorage.setItem(LS_WORKBENCH, state.workbenchExpanded ? "1" : "0");
}

function toIsoDate(value) {
  if (!value) return "";
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) return value;
  const date = value instanceof Date ? new Date(value) : new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function addDays(iso, delta) {
  const date = new Date(`${iso}T00:00:00`);
  if (Number.isNaN(date.getTime())) return iso;
  date.setDate(date.getDate() + delta);
  return toIsoDate(date);
}

function getNewestDate() {
  return state.index?.days?.[0]?.date || toIsoDate(new Date());
}

function generateDateRange(start, end) {
  if (!start || !end) return [];
  const out = [];
  let cursor = start <= end ? start : end;
  const limit = start <= end ? end : start;
  while (cursor <= limit) {
    out.push(cursor);
    cursor = addDays(cursor, 1);
  }
  return out;
}

function syncSettingsForm() {
  const mapping = {
    theme: "#setting-theme",
    density: "#setting-density",
    group: "#setting-group",
    cluster: "#setting-cluster",
    summary: "#setting-summary",
  };
  for (const [key, selector] of Object.entries(mapping)) {
    const el = $(selector);
    if (el) el.value = state.settings[key];
  }
}

function setSelectValue(selector, value, fallback = "") {
  const el = $(selector);
  if (!el) return;
  const desired = String(value ?? fallback ?? "");
  if (![...el.options].some((option) => option.value === desired) && desired) {
    el.appendChild(h("option", { value: desired }, desired));
  }
  el.value = desired || fallback || "";
}

function isSettingsModalOpen() {
  return !$("#settings-modal")?.hasAttribute("hidden");
}

function openSettingsModal() {
  const modal = $("#settings-modal");
  if (!modal) return;
  modal.removeAttribute("hidden");
  document.body.classList.add("modal-open");
  $("#settings-btn")?.setAttribute("aria-expanded", "true");
  applySettingsTab();
  window.setTimeout(() => {
    const activeTab = $(`.settings-tab[data-settings-tab="${state.settingsTab}"]`);
    const firstField = $(`[data-settings-tab-panel="${state.settingsTab}"] input, [data-settings-tab-panel="${state.settingsTab}"] select, [data-settings-tab-panel="${state.settingsTab}"] button`);
    (firstField || activeTab || $("#setting-theme"))?.focus();
  }, 0);
}

function closeSettingsModal() {
  const modal = $("#settings-modal");
  if (!modal) return;
  modal.setAttribute("hidden", "");
  document.body.classList.remove("modal-open");
  const button = $("#settings-btn");
  button?.setAttribute("aria-expanded", "false");
  button?.focus();
}

function applySettingsTab() {
  if (IS_STATIC_SITE && ["sources", "scoring", "tagging", "digest"].includes(state.settingsTab)) {
    state.settingsTab = "reading";
  }
  $$(".settings-tab").forEach((tab) => {
    const tabName = tab.getAttribute("data-settings-tab");
    const staticHidden = IS_STATIC_SITE && ["sources", "scoring", "tagging", "digest"].includes(tabName);
    if (staticHidden) tab.setAttribute("hidden", "");
    const active = !staticHidden && tabName === state.settingsTab;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-pressed", active ? "true" : "false");
  });
  $$("[data-settings-tab-panel]").forEach((panel) => {
    const panelName = panel.getAttribute("data-settings-tab-panel");
    panel.hidden = panelName !== state.settingsTab || (IS_STATIC_SITE && ["sources", "scoring", "tagging", "digest"].includes(panelName));
  });
}

function syncScoringEnabledButtons() {
  $$(`[data-setting="scoring-enabled"] button`).forEach((b) => {
    const isOn = String(Boolean(state.scoringConfig.enabled));
    b.setAttribute("aria-pressed", b.getAttribute("data-value") === isOn ? "true" : "false");
  });
}

function syncTaggingEnabledButtons() {
  $$(`[data-setting="tagging-enabled"] button`).forEach((b) => {
    const isOn = String(Boolean(state.taggingConfig.enabled));
    b.setAttribute("aria-pressed", b.getAttribute("data-value") === isOn ? "true" : "false");
  });
}

function syncDigestEnabledButtons() {
  $$(`[data-setting="digest-enabled"] button`).forEach((b) => {
    const isOn = String(Boolean(state.digestConfig.enabled));
    b.setAttribute("aria-pressed", b.getAttribute("data-value") === isOn ? "true" : "false");
  });
}

function syncRssSettings() {
  setSelectValue("#rss-max-items", state.scoringConfig.rss?.max_items || 80, "80");
  setSelectValue("#scoring-parallel-workers", state.scoringConfig.parallel_workers || 1, String(state.scoringConfig.parallel_workers || 1));
}

function setScoringConfig(scoring) {
  state.scoringConfig = scoring || { enabled: false, active_profile: null, profiles: [], parallel_workers: 1, rss: { enabled: true, max_items: 80 } };
  syncScoringEnabledButtons();
  syncRssSettings();
  renderProfileList();
}

function setTaggingConfig(tagging) {
  state.taggingConfig = tagging || { enabled: false, active_profile: null, profiles: [], parallel_workers: 1, max_pending_per_run: 50, allow_inherit_from_cluster: true };
  syncTaggingEnabledButtons();
  const pendingValue = state.taggingConfig.max_pending_per_run;
  setSelectValue("#tagging-max-pending", pendingValue == null ? 50 : pendingValue, String(pendingValue == null ? 50 : pendingValue));
  const parallelValue = state.taggingConfig.parallel_workers == null ? 1 : state.taggingConfig.parallel_workers;
  setSelectValue("#tagging-parallel-workers", parallelValue, String(parallelValue));
  const inheritEl = $("#tagging-allow-inherit");
  if (inheritEl) inheritEl.checked = Boolean(state.taggingConfig.allow_inherit_from_cluster);
  renderTaggingProfileList();
}

function setDigestConfig(digest) {
  state.digestConfig = digest || { enabled: false, active_profile: null, profiles: [], parallel_workers: 1, schedule: { time: "08:30" }, outputs: { web: true, obsidian: true } };
  syncDigestEnabledButtons();
  const scheduleInput = $("#digest-schedule-time");
  if (scheduleInput) scheduleInput.value = state.digestConfig.schedule?.time || "08:30";
  setSelectValue("#digest-parallel-workers", state.digestConfig.parallel_workers || 1, String(state.digestConfig.parallel_workers || 1));
  const obsidianOutput = $("#digest-output-obsidian");
  if (obsidianOutput) obsidianOutput.checked = Boolean(state.digestConfig.outputs?.obsidian);
  renderDigestProfileList();
}

function setSourceConfig(sourceConfig) {
  state.sourceConfig = sourceConfig || { sources: [], categories: [] };
  const sourceIds = new Set((state.sourceConfig.sources || []).map((row) => row.id));
  state.selectedSourceIds = new Set([...state.selectedSourceIds].filter((id) => sourceIds.has(id)));
  syncSourceCategoryDatalist();
  syncSourceFilterOptions();
  renderSourceConfigList();
}

function syncSourceCategoryDatalist() {
  const datalist = $("#source-category-options");
  if (!datalist) return;
  datalist.innerHTML = "";
  for (const category of state.sourceConfig.categories || []) {
    datalist.appendChild(h("option", { value: category.label }, category.label));
  }
}

function syncSourceFilterOptions() {
  const select = $("#source-filter-category");
  if (!select) return;
  const current = state.sourceFilterCategory || "";
  select.innerHTML = "";
  select.appendChild(h("option", { value: "" }, "全部分类"));
  for (const category of state.sourceConfig.categories || []) {
    select.appendChild(h("option", { value: category.label }, category.label));
  }
  if (![...select.options].some((option) => option.value === current)) {
    state.sourceFilterCategory = "";
  }
  select.value = state.sourceFilterCategory || "";
}

function sourceRuntime(source) {
  return source?.runtime || {};
}

function isFailedSource(source) {
  const runtime = sourceRuntime(source);
  return source?.type === "rss" && source?.enabled !== false && runtime.status === "failed";
}

function getFilteredSourceRows() {
  const keyword = state.sourceFilterSearch.trim().toLowerCase();
  const category = state.sourceFilterCategory;
  const status = state.sourceFilterStatus || "all";
  return (state.sourceConfig.sources || []).filter((source) => {
    const config = source.config || {};
    if (category && config.category !== category) return false;
    if (status === "enabled" && source.enabled === false) return false;
    if (status === "paused" && source.enabled !== false) return false;
    if (status === "failed" && !isFailedSource(source)) return false;
    if (!keyword) return true;
    const haystack = [
      source.id,
      source.name,
      config.feed_url,
      config.source_label,
      config.category,
    ].filter(Boolean).join(" ").toLowerCase();
    return haystack.includes(keyword);
  });
}

function sourceStatusMeta(source) {
  const runtime = sourceRuntime(source);
  if (source?.type !== "rss") return { label: "系统源", className: "status-external" };
  if (source?.enabled === false) return { label: "已暂停", className: "status-paused" };
  if (runtime.status === "ok") return { label: "正常", className: "status-ok" };
  return { label: "抓取失败", className: "status-failed" };
}

function setScoringStatus(status) {
  state.scoringStatus = status || { running: false, paused: false, queued_dates: [], pending_jobs: [], backfill_jobs: [], current_job: null, current_jobs: [], recent_jobs: [], config: { enabled: false, active_profile: null } };
  const currentJobId = state.scoringStatus.current_job?.job_id || null;
  const pendingJobIds = (state.scoringStatus.pending_jobs || []).map((job) => job.job_id).filter(Boolean);
  const recentJobIds = (state.scoringStatus.recent_jobs || []).map((job) => job.job_id).filter(Boolean);
  const preferredJobId = currentJobId || pendingJobIds[0] || recentJobIds[0] || null;
  if (preferredJobId && ![currentJobId, ...pendingJobIds, ...recentJobIds].includes(state.selectedRuntimeJobId)) {
    state.selectedRuntimeJobId = preferredJobId;
  } else if (!state.selectedRuntimeJobId) {
    state.selectedRuntimeJobId = preferredJobId;
  }
  renderScoringRuntime();
}

function setTaggingStatus(status) {
  state.taggingStatus = status || { running: false, paused: false, queued_dates: [], pending_jobs: [], backfill_jobs: [], current_job: null, recent_jobs: [], config: { enabled: false, active_profile: null } };
  const currentJobId = state.taggingStatus.current_job?.job_id || null;
  const pendingJobIds = (state.taggingStatus.pending_jobs || []).map((job) => job.job_id).filter(Boolean);
  const recentJobIds = (state.taggingStatus.recent_jobs || []).map((job) => job.job_id).filter(Boolean);
  const preferredJobId = currentJobId || pendingJobIds[0] || recentJobIds[0] || null;
  if (preferredJobId && ![currentJobId, ...pendingJobIds, ...recentJobIds].includes(state.selectedTaggingJobId)) {
    state.selectedTaggingJobId = preferredJobId;
  } else if (!state.selectedTaggingJobId) {
    state.selectedTaggingJobId = preferredJobId;
  }
  renderTaggingRuntime();
}

function setDigestStatus(status) {
  state.digestStatus = status || { running: false, paused: false, queued_dates: [], pending_jobs: [], current_job: null, current_jobs: [], recent_jobs: [], config: { enabled: false, active_profile: null, schedule: { time: "08:30" }, outputs: { web: true, obsidian: true } } };
  const currentJobIds = (state.digestStatus.current_jobs || []).map((job) => job.job_id).filter(Boolean);
  const pendingJobIds = (state.digestStatus.pending_jobs || []).map((job) => job.job_id).filter(Boolean);
  const recentJobIds = (state.digestStatus.recent_jobs || []).map((job) => job.job_id).filter(Boolean);
  const preferredJobId = currentJobIds[0] || pendingJobIds[0] || recentJobIds[0] || null;
  if (preferredJobId && ![...currentJobIds, ...pendingJobIds, ...recentJobIds].includes(state.selectedDigestJobId)) {
    state.selectedDigestJobId = preferredJobId;
  } else if (!state.selectedDigestJobId) {
    state.selectedDigestJobId = preferredJobId;
  }
  renderDigestRuntime();
}

function toggleSetValue(set, value) {
  if (value === "全部") {
    set.clear();
    return;
  }
  set.has(value) ? set.delete(value) : set.add(value);
}

function clearAllFilters() {
  state.activeCategories.clear();
  state.activeSources.clear();
  state.activeEntityTags.clear();
  state.activeTopicTags.clear();
  state.activeScoreLabels.clear();
  state.activeItemStates.clear();
  clearDayQuickFilters();
  Object.values(state.activeLevels).forEach((set) => set.clear());
  state.selectedDate = null;
  resetTimelineWindow();
  state.search = "";
  const searchEl = $("#search");
  if (searchEl) searchEl.value = "";
}

function getItemScoreLabel(it) {
  return it.pm_label || (it.pm_score_status === "pending" ? "未评分" : "未评分");
}

function getDayQuickFilters(date) {
  const quick = state.dayQuickFilters || { date: null, source: "", score: "" };
  if (!date || !(quick.date == null || quick.date === date)) return { date, source: "", score: "" };
  return { date, source: quick.source || "", score: quick.score || "" };
}

function countDayQuickFilters(date) {
  const quick = getDayQuickFilters(date);
  return (quick.source ? 1 : 0) + (quick.score ? 1 : 0);
}

function clearDayQuickFilters(date = null) {
  if (date && state.dayQuickFilters?.date != null && state.dayQuickFilters.date !== date) return;
  state.dayQuickFilters = { date: null, source: "", score: "" };
}

async function setDayQuickFilter(date, key, value) {
  const current = getDayQuickFilters(date);
  const next = current[key] === value ? "" : value;
  state.dayQuickFilters = {
    date: null,
    source: key === "source" ? next : current.source,
    score: key === "score" ? next : current.score,
  };
  renderSidebar();
  await renderTimeline();
}

async function clearCurrentDayQuickFilters(date) {
  clearDayQuickFilters(date);
  renderSidebar();
  await renderTimeline();
}

function humanizeBackfillReason(reason) {
  return {
    scoring_disabled: "评分开关未开启",
    tagging_disabled: "标签开关未开启",
    active_profile_missing: "还没有可用的模型配置",
  }[reason] || reason || "";
}

function formatRuntimeTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return `${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function formatIndexTimestamp(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${month}-${day} ${hours}:${minutes}`;
}

function latestUpdateText() {
  const label = formatIndexTimestamp(state.index?.generated_at);
  return label ? `最近更新 ${label}` : "等待更新";
}

function humanizeSourceProblem(problem) {
  return {
    auth_required: "飞书授权失效",
    network_timeout: "飞书接口超时",
    proxy_error: "代理/网络异常",
    sync_lagging: "小时同步落后",
    unknown_error: "同步失败",
  }[problem] || "同步异常";
}

function renderSourceAlert() {
  const shell = $("#sync-alert");
  const title = $("#sync-alert-title");
  const text = $("#sync-alert-text");
  const openAuth = $("#sync-auth-open");
  const reauthBtn = $("#sync-reauth-btn");
  const catchupBtn = $("#sync-catchup-btn");
  const refreshBtn = $("#sync-alert-refresh-btn");
  if (!shell || !title || !text || !openAuth || !reauthBtn || !catchupBtn || !refreshBtn) return;

  const status = state.sourceStatus || {};
  const problem = status.problem;
  const session = status.auth_session || null;
  const issue = status.latest_issue || null;
  const lagHours = status.hourly_state?.lag_hours;
  const lastSuccessEnd = status.hourly_state?.last_success_end;
  const sessionStatus = session?.status || "";
  const sessionIsActive = ["pending", "authorizing"].includes(sessionStatus)
    || (sessionStatus === "authorized" && session?.resume_status === "running");
  const sessionNeedsAttention = sessionStatus === "error"
    || (sessionStatus === "authorized" && session?.resume_status === "error");
  const sessionShouldRender = sessionIsActive || sessionNeedsAttention;

  shell.hidden = !problem && !sessionShouldRender;
  if (shell.hidden) return;

  shell.setAttribute("data-kind", problem || session?.status || "notice");
  openAuth.hidden = true;
  reauthBtn.hidden = true;
  catchupBtn.hidden = true;
  refreshBtn.hidden = false;
  openAuth.removeAttribute("href");

  if (sessionShouldRender && ["pending", "authorizing", "authorized", "error"].includes(session.status)) {
    title.textContent = session.status === "authorized" ? "飞书授权已完成" : (session.status === "error" ? "飞书授权失败" : "等待飞书授权");
    text.textContent = formatAuthSessionMessage(session);
    if (session.verification_url) {
      openAuth.hidden = false;
      openAuth.href = session.verification_url;
    }
    if (session.status === "error") reauthBtn.hidden = false;
    if (session.status === "authorized" && (session.resume_status === "error" || status.recommended_action === "catch_up")) catchupBtn.hidden = false;
    return;
  }

  title.textContent = humanizeSourceProblem(problem);
  if (problem === "auth_required") {
    text.textContent = `当前飞书授权不可用${status.auth?.user_name ? `（账号 ${status.auth.user_name}）` : ""}，请重新授权后继续同步。`;
    reauthBtn.hidden = false;
  } else if (problem === "sync_lagging") {
    text.textContent = `当前小时同步落后 ${lagHours || 0} 个整点窗口${lastSuccessEnd ? ` · 上次成功到 ${formatRuntimeTime(lastSuccessEnd)}` : ""}。可以直接继续补跑。`;
    catchupBtn.hidden = false;
  } else {
    text.textContent = `${issue?.message || "同步出现异常"}${lastSuccessEnd ? ` · 上次成功到 ${formatRuntimeTime(lastSuccessEnd)}` : ""}`;
    catchupBtn.hidden = false;
    if (!status.auth?.ok || status.auth?.token_status !== "valid") reauthBtn.hidden = false;
  }
}

function formatAuthSessionMessage(session) {
  if (!session) return "请在浏览器里完成飞书授权。";
  const dates = session.resume_result?.dates || [];
  if (session.status === "authorized" && session.resume_status === "done") {
    return dates.length ? `授权成功，已继续补跑 ${dates.length} 天。` : "授权成功，当前没有需要补跑的缺口。";
  }
  if (session.status === "authorized" && session.resume_status === "running") {
    return "授权成功，正在继续补跑…";
  }
  if (session.status === "authorized" && session.resume_status === "error") {
    return session.resume_error ? `授权成功，但补跑失败：${session.resume_error}` : "授权成功，但补跑失败。";
  }
  return session.message || "请在浏览器里完成飞书授权。";
}

function currentIndexSignature(index = state.index) {
  if (!index) return "";
  return JSON.stringify({
    generated_at: index.generated_at || "",
    days: (index.days || []).map((day) => [day.date, day.items, day.cards, day.scored_items || 0]),
  });
}

function runtimeCounts(job) {
  const counts = job?.counts || {};
  return {
    total: counts.total || 0,
    done: counts.done || 0,
    pending: counts.pending || 0,
    error: counts.error || 0,
    skipped: counts.skipped || 0,
  };
}

function formatRuntimeRange(job) {
  if (!job) return "";
  const start = job.request_start_date;
  const end = job.request_end_date;
  const days = job.request_total_days;
  if (!start && !end) return "";
  if (start && end && start !== end) return `范围 ${start} → ${end}${days ? ` · ${days} 天` : ""}`;
  return `范围 ${start || end}${days && days > 1 ? ` · ${days} 天` : ""}`;
}

function formatRuntimeProgress(job) {
  if (!job?.request_total_days || job.request_total_days <= 1 || !job.request_day_index) return "";
  return `当前第 ${job.request_day_index}/${job.request_total_days} 天`;
}

function renderRuntimeRangeLine(job) {
  const range = formatRuntimeRange(job);
  const progress = formatRuntimeProgress(job);
  if (!range && !progress) return null;
  return h("div", { class: "runtime-job-range" }, [range, progress].filter(Boolean).join(" · "));
}

function taskRuntimeState(task) {
  if (task === "scoring") return state.scoringStatus;
  if (task === "tagging") return state.taggingStatus;
  return state.digestStatus;
}

function taskQueueApi(task) {
  return `/api/${task}/queue`;
}

async function refreshTaskRuntime(task) {
  if (task === "scoring") return loadScoringStatus();
  if (task === "tagging") return loadTaggingStatus();
  return loadDigestStatus();
}

function selectedTaskJob(task) {
  if (task === "scoring") return getSelectedRuntimeJob();
  if (task === "tagging") return getSelectedTaggingJob();
  return getSelectedDigestJob();
}

function taskStatusSelector(task) {
  if (task === "scoring") return "#backfill-status";
  if (task === "tagging") return "#tagging-backfill-status";
  return "#digest-run-status";
}

function taskNoun(task) {
  return { scoring: "评分", tagging: "标签", digest: "日报" }[task] || task;
}

async function runQueueAction(task, payload, successText) {
  const selector = taskStatusSelector(task);
  setStatusText(selector, `${taskNoun(task)}任务处理中…`, "loading");
  const { res, body } = await apiJson(taskQueueApi(task), payload);
  if (!res.ok || body.ok === false) {
    setStatusText(selector, body.error || `HTTP ${res.status}`, "error");
    await refreshTaskRuntime(task).catch(() => {});
    return { ok: false, body };
  }
  setStatusText(selector, successText || "已更新队列", "success");
  await refreshTaskRuntime(task).catch(() => {});
  return { ok: true, body };
}

function getSelectedDigestJob() {
  const currentJobs = state.digestStatus.current_jobs || [];
  const currentJob = currentJobs.find((job) => job.job_id === state.selectedDigestJobId) || currentJobs[0] || null;
  if (currentJob && currentJob.job_id === state.selectedDigestJobId) return currentJob;
  const pendingJob = (state.digestStatus.pending_jobs || []).find((job) => job.job_id === state.selectedDigestJobId);
  if (pendingJob) return pendingJob;
  return (state.digestStatus.recent_jobs || []).find((job) => job.job_id === state.selectedDigestJobId) || currentJob || null;
}

function renderQueueControls(task, mountSelector) {
  const mount = $(mountSelector);
  if (!mount) return;
  const runtime = taskRuntimeState(task);
  const job = selectedTaskJob(task);
  const requestId = job?.request_id || null;
  const dateLabel = job?.date || "选中日期";
  mount.innerHTML = "";
  mount.appendChild(h("button", {
    class: "mini-btn",
    type: "button",
    onclick: async () => {
      await runQueueAction(task, { action: runtime.paused ? "resume" : "pause" }, runtime.paused ? "已继续排队" : "已暂停接新任务");
    },
  }, runtime.paused ? "继续" : "暂停"));
  mount.appendChild(h("button", {
    class: "mini-btn",
    type: "button",
    disabled: !job?.date,
    onclick: async () => {
      await runQueueAction(task, { action: "retry_date", date: job.date, kind: job.kind || "backfill", force: Boolean(job.force) }, `已把 ${dateLabel} 插到最前`);
    },
  }, "重试当前日期"));
  mount.appendChild(h("button", {
    class: "mini-btn",
    type: "button",
    disabled: !job?.date,
    onclick: async () => {
      await runQueueAction(task, { action: "retry_failed", date: job.date, kind: job.kind || "backfill" }, `已把 ${dateLabel} 的失败项放到最前`);
    },
  }, "重跑失败项"));
  mount.appendChild(h("button", {
    class: "mini-btn danger",
    type: "button",
    disabled: !job?.date,
    onclick: async () => {
      await runQueueAction(task, { action: "remove_dates", dates: [job.date] }, `已从队列移除 ${dateLabel}`);
    },
  }, "删除排队项"));
  mount.appendChild(h("button", {
    class: "mini-btn danger",
    type: "button",
    disabled: !requestId,
    onclick: async () => {
      await runQueueAction(task, { action: "remove_request", request_id: requestId }, "已删除整组排队项");
    },
  }, "删除整组"));
}

function formatLabelBreakdown(labelCounts = {}) {
  return ["必读", "值得读", "可选读", "略过"]
    .filter((label) => (labelCounts[label] || 0) > 0)
    .map((label) => `${label} ${labelCounts[label]}`)
    .join(" · ");
}

function renderRuntimeMetric(label, count, tone) {
  return h("div", { class: `runtime-metric runtime-metric-${tone}` }, [
    h("div", { class: "runtime-metric-label" }, label),
    h("div", { class: "runtime-metric-value" }, String(count || 0)),
  ]);
}

function renderRuntimeExamples(rows, emptyText, kind = "done") {
  if (!rows?.length) {
    return h("div", { class: "runtime-empty" }, emptyText);
  }
  return h("div", { class: "runtime-example-list" }, rows.map((row) =>
    h("article", { class: `runtime-example runtime-example-${kind}` }, [
      h("div", { class: "runtime-example-title" }, cleanTitle(row.title || row.item_id || "未命名条目")),
      h("div", { class: "runtime-example-meta" }, [
        row.pm_label ? h("span", { class: `score-pill score-${slugify(row.pm_label)}` }, `${row.pm_label}${row.pm_score != null ? ` ${row.pm_score}` : ""}`) : null,
        row.segment ? h("span", {}, row.segment) : null,
        row.source ? h("span", { class: "source" }, row.source) : null,
      ].filter(Boolean)),
      row.pm_reason ? h("div", { class: "runtime-example-copy" }, row.pm_reason) : null,
      row.pm_error ? h("div", { class: "runtime-example-copy runtime-example-error" }, row.pm_error) : null,
    ])
  ));
}

function renderRuntimeDetailCard(title, hint, content) {
  return h("section", { class: "runtime-detail-card" }, [
    h("div", { class: "runtime-detail-head" }, [
      h("strong", {}, title),
      hint ? h("p", {}, hint) : null,
    ]),
    content,
  ]);
}

function normalizeItemState(raw = {}) {
  return {
    starred: Boolean(raw.starred),
    read_later: Boolean(raw.read_later),
    read: Boolean(raw.read),
    note: raw.note || null,
    updated_at: raw.updated_at || null,
  };
}

function hasStoredItemState(itemState) {
  return Boolean(
    itemState.starred ||
    itemState.read_later ||
    itemState.read ||
    String(itemState.note || "").trim(),
  );
}

function getItemState(itemId) {
  return normalizeItemState(state.userStates.get(itemId));
}

function setLocalItemState(itemId, nextState) {
  const normalized = normalizeItemState(nextState);
  if (hasStoredItemState(normalized)) {
    state.userStates.set(itemId, normalized);
  } else {
    state.userStates.delete(itemId);
  }
}

async function loadUserStates(itemIds = null) {
  if (IS_STATIC_SITE) return { states: {} };
  const query = Array.isArray(itemIds) && itemIds.length
    ? `?${new URLSearchParams(itemIds.flatMap((itemId) => [["item_id", itemId]])).toString()}`
    : "";
  const signal = typeof AbortSignal !== "undefined" && AbortSignal.timeout
    ? AbortSignal.timeout(2500)
    : undefined;
  const res = await fetch(`/api/user-state${query}`, { cache: "no-store", signal });
  if (!res.ok) throw new Error(`读取个人状态失败：${res.status}`);
  const payload = await res.json();
  const incoming = new Map(Object.entries(payload.states || {}).map(([itemId, value]) => [itemId, normalizeItemState(value)]));
  if (Array.isArray(itemIds) && itemIds.length) {
    for (const itemId of itemIds) state.userStates.delete(itemId);
    for (const [itemId, value] of incoming.entries()) state.userStates.set(itemId, value);
  } else {
    state.userStates = incoming;
  }
  return payload;
}

async function updateItemState(itemId, patch) {
  if (IS_STATIC_SITE) {
    const current = getItemState(itemId);
    const next = { ...current, ...(patch || {}), updated_at: new Date().toISOString() };
    setLocalItemState(itemId, next);
    return next;
  }
  const res = await fetch(`/api/items/${encodeURIComponent(itemId)}/state`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok || body.ok === false) {
    throw new Error(body.error || `HTTP ${res.status}`);
  }
  setLocalItemState(itemId, body.state || {});
  return body.state;
}

async function loadRuntimeJobDetail(jobId) {
  if (!jobId || state.runtimeJobDetails.has(jobId)) return state.runtimeJobDetails.get(jobId);
  const res = await fetch(`/api/scoring/jobs/${encodeURIComponent(jobId)}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`读取任务详情失败：${res.status}`);
  const detail = await res.json();
  state.runtimeJobDetails.set(jobId, detail);
  return detail;
}

function getSelectedRuntimeJob() {
  const currentJob = state.scoringStatus.current_job;
  if (currentJob?.job_id && state.selectedRuntimeJobId === currentJob.job_id) return currentJob;
  const pendingJob = (state.scoringStatus.pending_jobs || []).find((job) => job.job_id === state.selectedRuntimeJobId);
  if (pendingJob) return pendingJob;
  return (state.scoringStatus.recent_jobs || []).find((job) => job.job_id === state.selectedRuntimeJobId) || currentJob || null;
}

function runtimeRowsForTab(detail, tab) {
  const details = detail?.details || {};
  if (tab === "pending") return details.pending_examples || [];
  if (tab === "error") return details.error_examples || [];
  return details.done_examples || [];
}

function renderRuntimeDetailTabs(detail) {
  const wrap = $("#runtime-detail-tabs");
  if (!wrap) return;
  const counts = {
    done: detail?.counts?.done || 0,
    pending: detail?.counts?.pending || 0,
    error: detail?.counts?.error || 0,
  };
  const labels = { done: "Done", pending: "Pending", error: "Error" };
  wrap.innerHTML = "";
  ["done", "pending", "error"].forEach((tab) => {
    wrap.appendChild(h("button", {
      class: `runtime-tab ${state.runtimeDetailTab === tab ? "active" : ""}`,
      type: "button",
      onclick: () => {
        state.runtimeDetailTab = tab;
        renderScoringRuntime();
      },
    }, `${labels[tab]} · ${counts[tab]}`));
  });
}

function renderRuntimeDetailBody(job, detail) {
  const detailGrid = $("#runtime-detail-grid");
  if (!detailGrid) return;
  detailGrid.innerHTML = "";
  if (!job) {
    detailGrid.appendChild(renderRuntimeDetailCard("任务详情", "还没有可查看的任务。", h("div", { class: "runtime-empty" }, "先运行一次评分或补跑任务。")));
    return;
  }

  const isCurrentRunning = state.scoringStatus.current_job?.job_id && state.scoringStatus.current_job.job_id === job.job_id;
  if (!detail && isCurrentRunning) {
    detailGrid.appendChild(renderRuntimeDetailCard(
      "当前任务",
      "运行中的任务这里先展示摘要；完成后可点击历史任务查看完整明细。",
      h("div", { class: "runtime-empty" }, "当前任务仍在运行，稍后会进入历史列表。"),
    ));
    return;
  }
  if (!detail && ["queued", "cooldown"].includes(job.status)) {
    detailGrid.appendChild(renderRuntimeDetailCard(
      "排队中的任务",
      job.status === "cooldown" ? "这个日期正在冷却，到了重试时间会自动继续。" : "这个日期还没开始跑，你可以继续插队、删除或暂停。",
      h("div", { class: "runtime-empty" }, job.status === "cooldown" ? `预计 ${formatRuntimeTime(job.resume_at) || "稍后"} 重试。` : "还没开始执行。"),
    ));
    return;
  }
  if (!detail) {
    detailGrid.appendChild(renderRuntimeDetailCard("任务详情", "正在加载任务明细…", h("div", { class: "runtime-empty" }, "请稍候。")));
    return;
  }

  const labelBreakdown = formatLabelBreakdown(detail.details?.label_counts || {});
  const rows = runtimeRowsForTab(detail, state.runtimeDetailTab);
  const hintMap = {
    done: labelBreakdown ? `标签分布：${labelBreakdown}` : "当前没有已完成条目。",
    pending: detail.details?.pending_hint || "仍在等待评分返回的条目。",
    error: detail.details?.error_hint || "模型返回异常或结果解析失败的条目。",
  };
  const titleMap = { done: "Done 明细", pending: "Pending 明细", error: "Error 明细" };
  const emptyMap = {
    done: "这个任务里没有已完成条目。",
    pending: "这个任务里没有待处理条目。",
    error: "这个任务里没有错误条目。",
  };

  detailGrid.appendChild(renderRuntimeDetailCard(
    `${titleMap[state.runtimeDetailTab]} · ${job.date || "—"}`,
    hintMap[state.runtimeDetailTab],
    renderRuntimeExamples(rows, emptyMap[state.runtimeDetailTab], state.runtimeDetailTab),
  ));
}

function renderScoringRuntime() {
  const summary = $("#scoring-runtime-summary");
  const current = $("#scoring-runtime-current");
  const controls = $("#scoring-runtime-controls");
  const breakdown = $("#runtime-breakdown");
  const tabs = $("#runtime-detail-tabs");
  const detailGrid = $("#runtime-detail-grid");
  const list = $("#scoring-runtime-list");
  if (!summary || !current || !list || !breakdown || !detailGrid || !tabs || !controls) return;

  const cfg = state.scoringStatus.config || {};
  const running = Boolean(state.scoringStatus.running);
  const queued = state.scoringStatus.queued_dates || [];
  const deferredCount = Number(state.scoringStatus.deferred_queue_size || (state.scoringStatus.deferred_dates || []).length || 0);
  const currentJob = state.scoringStatus.current_job;
  const pendingJobs = state.scoringStatus.pending_jobs || [];
  const recentJobs = state.scoringStatus.recent_jobs || [];
  const focusJob = getSelectedRuntimeJob();
  const focusCounts = runtimeCounts(focusJob);
  const currentCounts = runtimeCounts(currentJob);

  const configLabel = cfg.enabled
    ? `评分已开启 · 当前模型 ${cfg.active_profile || "未选择"} · 并发 ${cfg.parallel_workers || state.scoringConfig.parallel_workers || 1}`
    : "评分未开启 · 补跑会被直接拒绝";
  const queueLabel = queued.length ? `活跃队列 ${queued.length} 天` : "当前无活跃队列";
  summary.textContent = `${configLabel} · ${queueLabel}`;
  if (deferredCount) summary.textContent += ` · 积压池 ${deferredCount} 天`;
  if (state.scoringStatus.paused) summary.textContent += " · 已暂停";

  if (currentJob) {
    if (currentJob.status === "cooldown") {
      current.textContent = `冷却中：${currentJob.date} · 新完成 ${currentCounts.done}/${currentCounts.total} · 已跳过 ${currentCounts.skipped} · 待处理 ${currentCounts.pending} · 预计 ${formatRuntimeTime(currentJob.resume_at) || "稍后"} 重试`;
    } else {
      current.textContent = `进行中：${currentJob.date} · 新完成 ${currentCounts.done}/${currentCounts.total} · 已跳过 ${currentCounts.skipped} · 待处理 ${currentCounts.pending} · 错误 ${currentCounts.error}`;
    }
  } else if (running) {
    current.textContent = "评分线程正在运行…";
  } else if (state.scoringStatus.paused) {
    current.textContent = queued.length ? `已暂停：活跃队列里还有 ${queued.length} 天，继续后会接着跑。` : "已暂停：当前没有排队任务。";
  } else if (deferredCount) {
    current.textContent = queued.length
      ? `当前先保留 ${queued.length} 天在活跃队列，剩余 ${deferredCount} 天放在积压池，避免一下子把评分线程塞满。`
      : `当前没有活跃队列，积压池里还有 ${deferredCount} 天，系统会按空位自动补进来。`;
  } else {
    current.textContent = recentJobs.length ? "最近任务见下方" : "还没有补跑记录";
  }

  breakdown.innerHTML = "";
  breakdown.append(
    renderRuntimeMetric("Done", focusCounts.done, "done"),
    renderRuntimeMetric("Pending", focusCounts.pending, "pending"),
    renderRuntimeMetric("Error", focusCounts.error, "error"),
    renderRuntimeMetric("Skipped", focusCounts.skipped, "skipped"),
  );

  renderRuntimeDetailTabs(state.runtimeJobDetails.get(focusJob?.job_id) || focusJob);
  renderRuntimeDetailBody(focusJob, state.runtimeJobDetails.get(focusJob?.job_id));
  renderQueueControls("scoring", "#scoring-runtime-controls");

  list.innerHTML = "";
  if (currentJob) {
    list.appendChild(h("div", { class: "runtime-list-kicker" }, "当前任务"));
  }
  if (currentJob) {
    list.appendChild(h("button", {
      class: `runtime-job runtime-job-button ${state.selectedRuntimeJobId === currentJob.job_id ? "active" : ""} runtime-${currentJob.status || "idle"}`,
      type: "button",
      onclick: () => {
        state.selectedRuntimeJobId = currentJob.job_id;
        renderScoringRuntime();
      },
    }, [
      h("div", { class: "runtime-job-main" }, [
        h("strong", {}, currentJob.date || "—"),
        h("span", { class: "runtime-job-status" }, currentJob.status === "cooldown" ? "冷却中" : "进行中"),
      ]),
      renderRuntimeRangeLine(currentJob),
      h("div", { class: "runtime-job-meta" }, [
        `done ${currentCounts.done || 0}`, " · ",
        `pending ${currentCounts.pending || 0}`, " · ",
        `error ${currentCounts.error || 0}`, " · ",
        `skipped ${currentCounts.skipped || 0}`,
      ]),
      formatLabelBreakdown(currentJob.label_counts || {}) ? h("div", { class: "runtime-job-breakdown" }, formatLabelBreakdown(currentJob.label_counts || {})) : null,
    ].filter(Boolean)));
  }
  if (pendingJobs.length) {
    list.appendChild(h("div", { class: "runtime-list-kicker" }, "排队中"));
  }
  for (const job of pendingJobs) {
    list.appendChild(h("button", {
      class: `runtime-job runtime-job-button ${state.selectedRuntimeJobId === job.job_id ? "active" : ""} runtime-${job.status || "queued"}`,
      type: "button",
      onclick: () => {
        state.selectedRuntimeJobId = job.job_id;
        renderScoringRuntime();
      },
    }, [
      h("div", { class: "runtime-job-main" }, [
        h("strong", {}, job.date || "—"),
        h("span", { class: "runtime-job-status" }, job.status === "cooldown" ? "冷却中" : "排队中"),
      ]),
      renderRuntimeRangeLine(job),
      h("div", { class: "runtime-job-meta" }, [
        job.force ? "force" : "增量",
        job.resume_at ? ` · 预计 ${formatRuntimeTime(job.resume_at) || "稍后"} 重试` : "",
      ].join("")),
    ]));
  }
  if (recentJobs.length) {
    list.appendChild(h("div", { class: "runtime-list-kicker" }, "最近任务"));
  }
  for (const job of recentJobs) {
    const counts = runtimeCounts(job);
    const breakdownText = formatLabelBreakdown(job.label_counts || job.details?.label_counts || {});
    list.appendChild(h("button", {
      class: `runtime-job runtime-job-button ${state.selectedRuntimeJobId === job.job_id ? "active" : ""} runtime-${job.status || "idle"}`,
      type: "button",
      onclick: async () => {
        state.selectedRuntimeJobId = job.job_id;
        renderScoringRuntime();
        try {
          await loadRuntimeJobDetail(job.job_id);
        } catch (_err) {
          // ignore and keep placeholder
        }
        renderScoringRuntime();
      },
    }, [
      h("div", { class: "runtime-job-main" }, [
        h("strong", {}, job.date || "—"),
        h("span", { class: "runtime-job-status" }, ({
          done: "已完成",
          failed: "失败",
          skipped: "已跳过",
        }[job.status] || job.status || "未知")),
      ]),
      renderRuntimeRangeLine(job),
      h("div", { class: "runtime-job-meta" }, [
        `done ${counts.done || 0}`,
        " · ",
        `pending ${counts.pending || 0}`,
        " · ",
        `error ${counts.error || 0}`,
        " · ",
        `skipped ${counts.skipped || 0}`,
        job.force ? " · force" : "",
        job.reason ? ` · ${humanizeBackfillReason(job.reason)}` : "",
        job.error ? ` · ${job.error}` : "",
      ]),
      job.finished_at ? h("div", { class: "runtime-job-time" }, `finished ${formatRuntimeTime(job.finished_at)}`) : null,
      breakdownText ? h("div", { class: "runtime-job-breakdown" }, breakdownText) : null,
    ].filter(Boolean)));
  }
}

function getSelectedTaggingJob() {
  const currentJob = state.taggingStatus.current_job;
  if (currentJob?.job_id && state.selectedTaggingJobId === currentJob.job_id) return currentJob;
  const pendingJob = (state.taggingStatus.pending_jobs || []).find((job) => job.job_id === state.selectedTaggingJobId);
  if (pendingJob) return pendingJob;
  return (state.taggingStatus.recent_jobs || []).find((job) => job.job_id === state.selectedTaggingJobId) || currentJob || null;
}

function renderTaggingRuntime() {
  const summary = $("#tagging-runtime-summary");
  const current = $("#tagging-runtime-current");
  const controls = $("#tagging-runtime-controls");
  const breakdown = $("#tagging-runtime-breakdown");
  const tabs = $("#tagging-runtime-tabs");
  const detailGrid = $("#tagging-runtime-detail-grid");
  const list = $("#tagging-runtime-list");
  if (!summary || !current || !breakdown || !tabs || !detailGrid || !list || !controls) return;
  const cfg = state.taggingStatus.config || {};
  const currentJob = state.taggingStatus.current_job;
  const pendingJobs = state.taggingStatus.pending_jobs || [];
  const recentJobs = state.taggingStatus.recent_jobs || [];
  const focusJob = getSelectedTaggingJob();
  const counts = focusJob?.counts || { done: 0, pending: 0, error: 0, inherited: 0, skipped: 0 };
  summary.textContent = cfg.enabled ? `标签已开启 · 当前模型 ${cfg.active_profile || "未选择"} · batch ${state.taggingConfig.batch_size || 1} · 并发 ${cfg.parallel_workers || state.taggingConfig.parallel_workers || 1} · max ${cfg.max_pending_per_run === 0 ? "不限" : (cfg.max_pending_per_run || 0)}` : "标签未开启";
  if (state.taggingStatus.paused) summary.textContent += " · 已暂停";
  current.textContent = currentJob
    ? `${currentJob.status === "cooldown" ? "冷却中" : "进行中"}：${currentJob.date}${[formatRuntimeRange(currentJob), formatRuntimeProgress(currentJob)].filter(Boolean).length ? ` · ${[formatRuntimeRange(currentJob), formatRuntimeProgress(currentJob)].filter(Boolean).join(" · ")}` : ""}`
    : state.taggingStatus.paused
      ? `已暂停：队列里还有 ${(state.taggingStatus.queued_dates || []).length} 天，继续后会接着跑。`
    : (recentJobs.length ? "最近任务见下方" : "还没有标签任务记录");
  breakdown.innerHTML = "";
  breakdown.append(renderRuntimeMetric("Done", counts.done, "done"), renderRuntimeMetric("Inherited", counts.inherited, "done"), renderRuntimeMetric("Pending", counts.pending, "pending"), renderRuntimeMetric("Error", counts.error, "error"));
  tabs.innerHTML = "";
  ["done", "inherited", "pending", "error"].forEach((tab) => {
    tabs.appendChild(h("button", { class: `runtime-tab ${state.taggingDetailTab === tab ? "active" : ""}`, type: "button", onclick: () => { state.taggingDetailTab = tab; renderTaggingRuntime(); } }, `${tab} · ${focusJob?.counts?.[tab] || 0}`));
  });
  const detail = state.taggingJobDetails.get(focusJob?.job_id) || focusJob;
  const rows = detail?.details?.[`${state.taggingDetailTab}_examples`] || [];
  detailGrid.innerHTML = "";
  detailGrid.appendChild(renderRuntimeDetailCard(`${state.taggingDetailTab} 明细`, detail?.details?.[`${state.taggingDetailTab}_hint`] || "", rows.length ? h("div", { class: "runtime-example-list" }, rows.map((row) => h("article", { class: `runtime-example runtime-example-${state.taggingDetailTab}` }, [h("div", { class: "runtime-example-title" }, cleanTitle(row.title || row.item_id || "未命名条目")), h("div", { class: "runtime-example-meta" }, [(row.entity_tags || []).length ? h("span", {}, (row.entity_tags || []).join(" · ")) : null, (row.topic_tags || []).length ? h("span", { class: "source" }, (row.topic_tags || []).join(" · ")) : null].filter(Boolean)), row.tag_reason ? h("div", { class: "runtime-example-copy" }, row.tag_reason) : null, row.tag_error ? h("div", { class: "runtime-example-copy runtime-example-error" }, row.tag_error) : null]))) : h("div", { class: "runtime-empty" }, "暂无明细")));
  renderQueueControls("tagging", "#tagging-runtime-controls");
  list.innerHTML = "";
  if (currentJob) list.appendChild(h("div", { class: "runtime-list-kicker" }, "当前任务"));
  if (currentJob) {
    list.appendChild(h("button", { class: `runtime-job runtime-job-button ${state.selectedTaggingJobId === currentJob.job_id ? "active" : ""} runtime-${currentJob.status || "idle"}`, type: "button", onclick: async () => { state.selectedTaggingJobId = currentJob.job_id; renderTaggingRuntime(); try { await loadTaggingJobDetail(currentJob.job_id); } catch (_err) {} renderTaggingRuntime(); } }, [h("div", { class: "runtime-job-main" }, [h("strong", {}, currentJob.date || "—"), h("span", { class: "runtime-job-status" }, currentJob.status || "未知")]), renderRuntimeRangeLine(currentJob), h("div", { class: "runtime-job-meta" }, [`done ${currentJob.counts?.done || 0}`, " · ", `inherited ${currentJob.counts?.inherited || 0}`, " · ", `pending ${currentJob.counts?.pending || 0}`, " · ", `error ${currentJob.counts?.error || 0}`, currentJob.force ? " · force" : ""]), currentJob.finished_at ? h("div", { class: "runtime-job-time" }, `finished ${formatRuntimeTime(currentJob.finished_at)}`) : null].filter(Boolean)));
  }
  if (pendingJobs.length) list.appendChild(h("div", { class: "runtime-list-kicker" }, "排队中"));
  for (const job of pendingJobs) {
    list.appendChild(h("button", { class: `runtime-job runtime-job-button ${state.selectedTaggingJobId === job.job_id ? "active" : ""} runtime-${job.status || "queued"}`, type: "button", onclick: () => { state.selectedTaggingJobId = job.job_id; renderTaggingRuntime(); } }, [h("div", { class: "runtime-job-main" }, [h("strong", {}, job.date || "—"), h("span", { class: "runtime-job-status" }, job.status === "cooldown" ? "冷却中" : "排队中")]), renderRuntimeRangeLine(job), h("div", { class: "runtime-job-meta" }, [job.force ? "force" : "增量", job.resume_at ? ` · 预计 ${formatRuntimeTime(job.resume_at) || "稍后"} 重试` : ""].join(""))].filter(Boolean)));
  }
  if (recentJobs.length) list.appendChild(h("div", { class: "runtime-list-kicker" }, "最近任务"));
  for (const job of recentJobs) {
    list.appendChild(h("button", { class: `runtime-job runtime-job-button ${state.selectedTaggingJobId === job.job_id ? "active" : ""} runtime-${job.status || "idle"}`, type: "button", onclick: async () => { state.selectedTaggingJobId = job.job_id; renderTaggingRuntime(); try { await loadTaggingJobDetail(job.job_id); } catch (_err) {} renderTaggingRuntime(); } }, [h("div", { class: "runtime-job-main" }, [h("strong", {}, job.date || "—"), h("span", { class: "runtime-job-status" }, job.status || "未知")]), renderRuntimeRangeLine(job), h("div", { class: "runtime-job-meta" }, [`done ${job.counts?.done || 0}`, " · ", `inherited ${job.counts?.inherited || 0}`, " · ", `pending ${job.counts?.pending || 0}`, " · ", `error ${job.counts?.error || 0}`, job.force ? " · force" : ""]), job.finished_at ? h("div", { class: "runtime-job-time" }, `finished ${formatRuntimeTime(job.finished_at)}`) : null].filter(Boolean)));
  }
}

function renderDigestRuntime() {
  const summary = $("#digest-runtime-summary");
  const current = $("#digest-runtime-current");
  const controls = $("#digest-runtime-controls");
  const list = $("#digest-runtime-list");
  if (!summary || !current || !list || !controls) return;
  const cfg = state.digestStatus.config || {};
  const currentJobs = state.digestStatus.current_jobs || [];
  const pendingJobs = state.digestStatus.pending_jobs || [];
  summary.textContent = cfg.enabled ? `日报已开启 · ${cfg.schedule?.time || "08:30"} Asia/Shanghai · 当前模型 ${cfg.active_profile || "降级模式"} · 并发 ${cfg.parallel_workers || state.digestConfig.parallel_workers || 1}` : "日报未开启（仍可手动生成）";
  if (state.digestStatus.paused) summary.textContent += " · 已暂停";
  current.textContent = state.digestStatus.current_job
    ? `进行中：${state.digestStatus.current_job.date}${currentJobs.length > 1 ? ` · 另有 ${currentJobs.length - 1} 个并行任务` : ""}${[formatRuntimeRange(state.digestStatus.current_job), formatRuntimeProgress(state.digestStatus.current_job)].filter(Boolean).length ? ` · ${[formatRuntimeRange(state.digestStatus.current_job), formatRuntimeProgress(state.digestStatus.current_job)].filter(Boolean).join(" · ")}` : ""}`
    : state.digestStatus.paused
      ? `已暂停：队列里还有 ${(state.digestStatus.queued_dates || []).length} 天。`
    : `最新目标：${state.digestStatus.latest_digest || "—"}`;
  renderQueueControls("digest", "#digest-runtime-controls");
  list.innerHTML = "";
  if (currentJobs.length) list.appendChild(h("div", { class: "runtime-list-kicker" }, "当前任务"));
  for (const job of currentJobs) {
    list.appendChild(h("button", {
      class: `runtime-job runtime-job-button ${state.selectedDigestJobId === job.job_id ? "active" : ""} runtime-${job.status || "idle"}`,
      type: "button",
      onclick: () => {
        state.selectedDigestJobId = job.job_id;
        renderDigestRuntime();
      },
    }, [h("div", { class: "runtime-job-main" }, [h("strong", {}, job.date || "—"), h("span", { class: "runtime-job-status" }, job.status || "运行中")]), renderRuntimeRangeLine(job), h("div", { class: "runtime-job-meta" }, [`kind ${job.kind || "manual"}`, job.force ? " · force" : "", job.counts?.sections != null ? ` · sections ${job.counts.sections}` : ""].filter(Boolean).join("")), job.finished_at ? h("div", { class: "runtime-job-time" }, `finished ${formatRuntimeTime(job.finished_at)}`) : null, job.error ? h("div", { class: "runtime-job-breakdown" }, job.error) : null].filter(Boolean)));
  }
  if (pendingJobs.length) list.appendChild(h("div", { class: "runtime-list-kicker" }, "排队中"));
  for (const job of pendingJobs) {
    list.appendChild(h("button", {
      class: `runtime-job runtime-job-button ${state.selectedDigestJobId === job.job_id ? "active" : ""} runtime-${job.status || "idle"}`,
      type: "button",
      onclick: () => {
        state.selectedDigestJobId = job.job_id;
        renderDigestRuntime();
      },
    }, [h("div", { class: "runtime-job-main" }, [h("strong", {}, job.date || "—"), h("span", { class: "runtime-job-status" }, job.status === "cooldown" ? "冷却中" : "排队中")]), renderRuntimeRangeLine(job), h("div", { class: "runtime-job-meta" }, [`kind ${job.kind || "manual"}`, job.force ? " · force" : "", job.resume_at ? ` · 预计 ${formatRuntimeTime(job.resume_at) || "稍后"} 重试` : ""].filter(Boolean).join(""))].filter(Boolean)));
  }
  if ((state.digestStatus.recent_jobs || []).length) list.appendChild(h("div", { class: "runtime-list-kicker" }, "最近任务"));
  for (const job of state.digestStatus.recent_jobs || []) {
    list.appendChild(h("button", {
      class: `runtime-job runtime-job-button ${state.selectedDigestJobId === job.job_id ? "active" : ""} runtime-${job.status || "idle"}`,
      type: "button",
      onclick: () => {
        state.selectedDigestJobId = job.job_id;
        renderDigestRuntime();
      },
    }, [h("div", { class: "runtime-job-main" }, [h("strong", {}, job.date || "—"), h("span", { class: "runtime-job-status" }, job.status || "运行中")]), renderRuntimeRangeLine(job), h("div", { class: "runtime-job-meta" }, [`kind ${job.kind || "manual"}`, job.force ? " · force" : "", job.counts?.sections != null ? ` · sections ${job.counts.sections}` : ""].filter(Boolean).join("")), job.finished_at ? h("div", { class: "runtime-job-time" }, `finished ${formatRuntimeTime(job.finished_at)}`) : null, job.error ? h("div", { class: "runtime-job-breakdown" }, job.error) : null].filter(Boolean)));
  }
}

function setStaticHiddenSection(label) {
  for (const section of $$(".side-section")) {
    const title = section.querySelector(".side-header > span")?.textContent?.trim();
    if (title === label) section.setAttribute("data-static-hide", "true");
  }
}

function applyStaticModeChrome() {
  document.documentElement.toggleAttribute("data-static-site", IS_STATIC_SITE);
  if (!IS_STATIC_SITE) return;

  state.sourceStatus = { ...STATIC_SITE_SOURCE_STATUS };
  setStaticHiddenSection("CATEGORIES");
  setStaticHiddenSection("WORTH READING");
  setStaticHiddenSection("ENTITY TAGS");
  setStaticHiddenSection("TOPIC TAGS");
  setStaticHiddenSection("MY LISTS");
  if (["sources", "scoring", "tagging", "digest"].includes(state.settingsTab)) {
    state.settingsTab = "reading";
  }

  const refreshBtn = $("#refresh-btn");
  if (refreshBtn) {
    refreshBtn.hidden = true;
    refreshBtn.disabled = true;
    refreshBtn.title = "静态部署由 GitHub Actions 自动更新";
  }
  const digestRefreshBtn = $("#digest-refresh-btn");
  if (digestRefreshBtn) {
    digestRefreshBtn.hidden = true;
    digestRefreshBtn.disabled = true;
  }
  const syncAlert = $("#sync-alert");
  if (syncAlert) syncAlert.hidden = true;

  ["sources", "scoring", "tagging", "digest"].forEach((tabName) => {
    $(`.settings-tab[data-settings-tab="${tabName}"]`)?.setAttribute("hidden", "");
    $(`[data-settings-tab-panel="${tabName}"]`)?.setAttribute("hidden", "");
  });
  const title = $("#settings-title");
  if (title) title.textContent = "阅读配置";
  const copy = $(".settings-modal-copy");
  if (copy) copy.textContent = "这是 GitHub Pages 静态站：只能调整本机阅读偏好，数据由 GitHub Actions 自动更新。";
}

async function applyPrivateBundle(bundle) {
  if (!bundle?.index) throw new Error("私有数据包里没有 index 数据");
  state.privateBundle = bundle;
  state.index = bundle.index || { days: [] };
  state.loadedDays.clear();
  for (const [date, day] of Object.entries(bundle.days || {})) {
    state.loadedDays.set(date, day || { date, cards: [], items: [] });
  }
  state.digestCache.clear();
  for (const [date, digest] of Object.entries(bundle.digests || {})) {
    state.digestCache.set(date, digest);
  }
  const digestDates = Object.keys(bundle.digests || {}).sort().reverse();
  state.digestIndex = bundle.digest_index || bundle.digestIndex || { dates: digestDates };
  state.sourceConfig = { sources: state.index.sources || [], categories: state.index.categories || [] };
  document.documentElement.setAttribute("data-private-unlocked", "true");
  clearAllFilters();
  state.timelineCount = TIMELINE_DAY_BATCH_COUNT;
  state.selectedDigestDate = getAvailableDigestDates()[0] || null;
  ensureDefaultFeedDate();
  renderSidebar();
  syncIndexMeta();
  syncPrimaryViews();
  await renderTimeline();
  setRefreshStatus("success", "已解锁私有版：本地评分、标签和日报已加载");
  window.setTimeout(() => setRefreshStatus(), 4500);
}

function isPrivateUnlockModalOpen() {
  return !$("#private-unlock-modal")?.hasAttribute("hidden");
}

function setPrivateUnlockStatus(message = "", tone = "") {
  const status = $("#private-unlock-status");
  if (!status) return;
  status.textContent = message;
  status.dataset.tone = tone;
}

function openPrivateUnlockModal() {
  if (state.privateBundle) {
    setRefreshStatus("success", "私有数据已经解锁");
    window.setTimeout(() => setRefreshStatus(), 2500);
    return;
  }
  const modal = $("#private-unlock-modal");
  if (!modal) return;
  modal.removeAttribute("hidden");
  document.body.classList.add("modal-open");
  $("#private-unlock-btn")?.setAttribute("aria-expanded", "true");
  setPrivateUnlockStatus("", "");
  window.setTimeout(() => $("#private-password")?.focus(), 0);
}

function closePrivateUnlockModal() {
  const modal = $("#private-unlock-modal");
  if (!modal) return;
  modal.setAttribute("hidden", "");
  document.body.classList.remove("modal-open");
  $("#private-unlock-btn")?.setAttribute("aria-expanded", "false");
  setPrivateUnlockStatus("", "");
  const password = $("#private-password");
  if (password) password.value = "";
  $("#private-unlock-btn")?.focus();
}

async function fetchPrivateBundleEnvelope() {
  const urls = ["private/private.enc", "data/private/private.enc"];
  let lastStatus = 0;
  for (const url of urls) {
    const res = await fetch(`${url}?v=${Date.now()}`, { cache: "no-store" });
    if (res.ok) return res.json();
    lastStatus = res.status;
    if (res.status !== 404) throw new Error(`读取私有包失败：${res.status}`);
  }
  throw new Error(lastStatus === 404 ? "还没有上传私有加密包" : "读取私有包失败");
}

async function unlockPrivateBundle(password) {
  const btn = $("#private-unlock-btn");
  if (!password) {
    setPrivateUnlockStatus("请输入密码。", "error");
    return;
  }
  try {
    btn?.setAttribute("data-state", "loading");
    $("#private-unlock-submit")?.setAttribute("disabled", "");
    setPrivateUnlockStatus("正在本地解密私有数据…", "loading");
    setRefreshStatus("loading", "正在本地解密私有数据…");
    const encrypted = await fetchPrivateBundleEnvelope();
    const bundle = await decryptPrivateBundle(encrypted, password);
    await applyPrivateBundle(bundle);
    btn?.setAttribute("data-state", "success");
    btn?.setAttribute("title", "私有数据已解锁");
    setPrivateUnlockStatus("已解锁。", "success");
    closePrivateUnlockModal();
  } catch (err) {
    btn?.setAttribute("data-state", "error");
    const message = err?.message || "私有数据解锁失败";
    setPrivateUnlockStatus(message, "error");
    setRefreshStatus("error", message);
    window.setTimeout(() => setRefreshStatus(), 5000);
  } finally {
    $("#private-unlock-submit")?.removeAttribute("disabled");
    const passwordInput = $("#private-password");
    if (passwordInput) passwordInput.value = "";
  }
}

// ─────────────────────────── Theme ───────────────────────────
function applyTheme() {
  const pref = state.settings.theme;
  const systemDark = matchMedia("(prefers-color-scheme: dark)").matches;
  const resolved = pref === "auto" ? (systemDark ? "dark" : "light") : pref;
  document.documentElement.setAttribute("data-theme", resolved);
}
matchMedia("(prefers-color-scheme: dark)").addEventListener?.("change", () => {
  if (state.settings.theme === "auto") applyTheme();
});

function applyDensity() { document.documentElement.setAttribute("data-density", state.settings.density); }
function applySummary() { document.documentElement.setAttribute("data-summary", state.settings.summary); }

// ─────────────────────────── Boot ────────────────────────────
async function main() {
  applyTheme(); applyDensity(); applySummary();
  applyStaticModeChrome();
  applySectionsInitial();
  wireSidebar();
  wireSettings();
  wireProfileForm();
  wireSourceConfigForm();
  wireBackfillForm();
  wireTopbar();
  wireSourceAlert();
  wireKeys();

  try {
    await Promise.all([
      reloadIndex(),
      loadSourceConfig(),
      loadScoringConfig(),
      loadTaggingConfig(),
      loadDigestConfig(),
      loadScoringStatus(),
      loadTaggingStatus(),
      loadDigestStatus(),
      loadSourceStatus().catch(() => {}),
      loadDigestIndex().catch(() => {}),
      loadUserStates().catch(() => {}),
    ]);
  } catch (e) {
    $("#timeline").prepend(h("div", { class: "empty" }, `未找到 data/index.json — 请先运行：python3 scripts/build-web-data.py`));
    return;
  }

  renderSidebar();
  syncIndexMeta();
  syncSettingsForm();
  fillSourceForm();
  fillProfileForm();
  fillTaggingProfileForm();
  fillDigestProfileForm();
  seedBackfillInputs();

  ensureDefaultFeedDate();
  await renderTimeline();
  window.setInterval(() => {
    if (document.hidden || IS_STATIC_SITE) return;
    Promise.all([
      loadScoringStatus().catch(() => {}),
      loadTaggingStatus().catch(() => {}),
      loadDigestStatus().catch(() => {}),
      loadSourceStatus().catch(() => {}),
    ]);
  }, 30000);
  window.setInterval(() => {
    if (document.hidden) return;
    checkForFreshData().catch(() => {});
  }, 300000);
}

function syncIndexMeta() {
  $("#tag-days").textContent = String(state.index.days.length).padStart(3, "0");
  $("#generated-at").textContent = `最近更新 · ${formatIndexTimestamp(state.index.generated_at) || "—"}`;
  setRefreshStatus();
}

function applySectionsInitial() {
  $$(".side-section").forEach((sec) => {
    const head = sec.querySelector(".side-header > span");
    const key = head && head.textContent.trim();
    if (!key) return;
    if (state.sections[key] != null) {
      sec.setAttribute("data-open", state.sections[key] ? "true" : "false");
    }
  });
}

function wireSidebar() {
  $$(".side-header").forEach((btn) => {
    btn.addEventListener("click", () => {
      const sec = btn.parentElement;
      const open = sec.getAttribute("data-open") === "true";
      sec.setAttribute("data-open", open ? "false" : "true");
      const key = btn.querySelector("span").textContent.trim();
      state.sections[key] = !open;
      saveSections();
    });
  });

  $("#clear-all-filters")?.addEventListener("click", () => {
    clearAllFilters();
    renderSidebar();
    renderTimeline();
  });

  $("#clear-sources")?.addEventListener("click", () => {
    state.activeSources.clear();
    renderSidebar();
    renderTimeline();
  });

  $("#clear-entity-tags")?.addEventListener("click", () => {
    state.activeEntityTags.clear();
    renderSidebar();
    renderTimeline();
  });

  $("#clear-topic-tags")?.addEventListener("click", () => {
    state.activeTopicTags.clear();
    renderSidebar();
    renderTimeline();
  });

  $("#clear-score-filters")?.addEventListener("click", () => {
    state.activeScoreLabels.clear();
    renderSidebar();
    renderTimeline();
  });

  $("#clear-item-state-filters")?.addEventListener("click", () => {
    state.activeItemStates.clear();
    renderSidebar();
    renderTimeline();
  });

  for (const dimension of SCORE_DIMENSIONS) {
    $(dimension.clear)?.addEventListener("click", () => {
      state.activeLevels[dimension.key]?.clear();
      renderSidebar();
      renderTimeline();
    });
  }
}

function apiJson(path, payload) {
  if (backendDisabledInStaticMode()) {
    return Promise.resolve({ res: { ok: false, status: 0 }, body: { ok: false, error: "静态站不支持后台操作" } });
  }
  return fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  }).then((res) => res.json().then((body) => ({ res, body })));
}

function wireSettings() {
  syncSettingsForm();
  applySettingsTab();
  $$(".settings-tab").forEach((tab) => {
    tab.addEventListener("click", () => {
      state.settingsTab = tab.getAttribute("data-settings-tab") || "reading";
      saveSettingsTab();
      applySettingsTab();
    });
  });
  const settingSelectors = {
    theme: "#setting-theme",
    density: "#setting-density",
    group: "#setting-group",
    cluster: "#setting-cluster",
    summary: "#setting-summary",
  };
  for (const [key, selector] of Object.entries(settingSelectors)) {
    $(selector)?.addEventListener("change", async (e) => {
      state.settings[key] = e.target.value;
      saveSettings();
      if (key === "theme") applyTheme();
      if (key === "density") applyDensity();
      if (key === "summary") applySummary();
      if (key === "group" || key === "cluster") await renderTimeline();
    });
  }

  $$(`[data-setting="scoring-enabled"] button`).forEach((btn) => {
    btn.addEventListener("click", async () => {
      const enabled = btn.getAttribute("data-value") === "true";
      const { body } = await apiJson("/api/config/scoring", { enabled });
      setScoringConfig(body.scoring);
      loadScoringStatus().catch(() => {});
    });
  });

  $$(`[data-setting="tagging-enabled"] button`).forEach((btn) => {
    btn.addEventListener("click", async () => {
      const enabled = btn.getAttribute("data-value") === "true";
      const { body } = await apiJson("/api/config/tagging", { enabled });
      setTaggingConfig(body.tagging);
      loadTaggingStatus().catch(() => {});
    });
  });

  $$(`[data-setting="digest-enabled"] button`).forEach((btn) => {
    btn.addEventListener("click", async () => {
      const enabled = btn.getAttribute("data-value") === "true";
      const { body } = await apiJson("/api/config/daily-digest", { enabled });
      setDigestConfig(body.daily_digest);
      loadDigestStatus().catch(() => {});
    });
  });

  $("#rss-max-items")?.addEventListener("change", async (e) => {
    const { body } = await apiJson("/api/config/scoring", { rss: { max_items: Number(e.target.value || 80) } });
    setScoringConfig(body.scoring);
    loadScoringStatus().catch(() => {});
  });

  $("#scoring-parallel-workers")?.addEventListener("change", async (e) => {
    const raw = e.target.value;
    const { body } = await apiJson("/api/config/scoring", { parallel_workers: raw === "" ? 1 : Number(raw) });
    setScoringConfig(body.scoring);
    setStatusText("#backfill-status", `评分并发已保存为 ${raw || 1} 路；当前正在跑的日期不打断，下一天起按新值生效。`, "success");
    loadScoringStatus().catch(() => {});
  });

  $("#tagging-max-pending")?.addEventListener("change", async (e) => {
    const raw = e.target.value;
    const { body } = await apiJson("/api/config/tagging", { max_pending_per_run: raw === "" ? 50 : Number(raw) });
    setTaggingConfig(body.tagging);
    loadTaggingStatus().catch(() => {});
  });

  $("#tagging-parallel-workers")?.addEventListener("change", async (e) => {
    const raw = e.target.value;
    const { body } = await apiJson("/api/config/tagging", { parallel_workers: raw === "" ? 1 : Number(raw) });
    setTaggingConfig(body.tagging);
    setStatusText("#tagging-backfill-status", `标签并发已保存为 ${raw || 1} 路；当前正在跑的日期不打断，下一天起按新值生效。`, "success");
    loadTaggingStatus().catch(() => {});
  });

  $("#tagging-allow-inherit")?.addEventListener("change", async (e) => {
    const { body } = await apiJson("/api/config/tagging", { allow_inherit_from_cluster: Boolean(e.target.checked) });
    setTaggingConfig(body.tagging);
    loadTaggingStatus().catch(() => {});
  });

  $("#digest-schedule-time")?.addEventListener("change", async (e) => {
    const { body } = await apiJson("/api/config/daily-digest", { schedule: { time: e.target.value || "08:30" } });
    setDigestConfig(body.daily_digest);
    loadDigestStatus().catch(() => {});
  });

  $("#digest-parallel-workers")?.addEventListener("change", async (e) => {
    const raw = e.target.value;
    const { body } = await apiJson("/api/config/daily-digest", { parallel_workers: raw === "" ? 1 : Number(raw) });
    setDigestConfig(body.daily_digest);
    setStatusText("#digest-run-status", `日报并发已保存为 ${raw || 1} 路；已经启动的任务不打断，后续任务按新值生效。`, "success");
    loadDigestStatus().catch(() => {});
  });

  $("#digest-output-obsidian")?.addEventListener("change", async (e) => {
    const { body } = await apiJson("/api/config/daily-digest", { outputs: { obsidian: Boolean(e.target.checked), web: true } });
    setDigestConfig(body.daily_digest);
    loadDigestStatus().catch(() => {});
  });
}

function profilePayload(prefix) {
  return {
    id: $(`#${prefix}-profile-id`).value.trim() || slugify($(`#${prefix}-profile-name`).value.trim()),
    name: $(`#${prefix}-profile-name`).value.trim(),
    provider: $(`#${prefix}-profile-provider`).value.trim(),
    base_url: $(`#${prefix}-profile-base-url`).value.trim(),
    api_key: $(`#${prefix}-profile-api-key`).value.trim(),
    model: $(`#${prefix}-profile-model`).value.trim(),
    batch_size: Number($(`#${prefix}-profile-batch-size`).value || 1),
    activate: true,
  };
}

function wireProfileForm() {
  $("#profile-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const { body } = await apiJson("/api/config/scoring/profiles", profilePayload("profile"));
    setScoringConfig(body.scoring);
    fillProfileForm();
    loadScoringStatus().catch(() => {});
  });
  $("#profile-reset")?.addEventListener("click", () => fillProfileForm());

  $("#tagging-profile-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const { body } = await apiJson("/api/config/tagging/profiles", profilePayload("tagging"));
    setTaggingConfig(body.tagging);
    fillTaggingProfileForm();
    loadTaggingStatus().catch(() => {});
  });
  $("#tagging-profile-reset")?.addEventListener("click", () => fillTaggingProfileForm());

  $("#digest-profile-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const { body } = await apiJson("/api/config/daily-digest/profiles", profilePayload("digest"));
    setDigestConfig(body.daily_digest);
    fillDigestProfileForm();
    loadDigestStatus().catch(() => {});
  });
  $("#digest-profile-reset")?.addEventListener("click", () => fillDigestProfileForm());
}

function sourcePayload() {
  return {
    id: $("#source-id")?.value.trim() || "",
    name: $("#source-name")?.value.trim() || "",
    type: "rss",
    enabled: Boolean($("#source-enabled")?.checked),
    config: {
      feed_url: $("#source-feed-url")?.value.trim() || "",
      source_label: $("#source-label")?.value.trim() || ($("#source-name")?.value.trim() || ""),
      category: $("#source-category")?.value.trim() || "📦 其他",
    },
    derived: ["web"],
    category_emoji: $("#source-category-emoji")?.value.trim() || "",
    with_web: true,
  };
}

function fillSourceForm(source = null) {
  const config = source?.config || {};
  $("#source-id").value = source?.id || "";
  $("#source-name").value = source?.name || "";
  $("#source-feed-url").value = config.feed_url || "";
  $("#source-label").value = config.source_label || source?.name || "";
  $("#source-category").value = config.category || "";
  const matchedCategory = (state.sourceConfig.categories || []).find((row) => row.label === (config.category || ""));
  $("#source-category-emoji").value = matchedCategory?.emoji || "";
  const enabled = $("#source-enabled");
  if (enabled) enabled.checked = source ? Boolean(source.enabled) : true;
  setStatusText("#source-config-status", "");
}

async function saveSourceAndRefresh(payload, statusSelector, successText) {
  const { res, body } = await apiJson("/api/config/sources", payload);
  if (!res.ok || body.ok === false) {
    setStatusText(statusSelector, body.error || `HTTP ${res.status}`, "error");
    return null;
  }
  setSourceConfig(body);
  await reloadIndex().catch(() => {});
  if (successText) setStatusText(statusSelector, successText, "success");
  return body;
}

async function bulkUpdateSources(payload, statusSelector, successText) {
  const { res, body } = await apiJson("/api/config/sources/bulk-update", payload);
  if (!res.ok || body.ok === false) {
    setStatusText(statusSelector, body.error || `HTTP ${res.status}`, "error");
    return null;
  }
  setSourceConfig(body);
  await reloadIndex().catch(() => {});
  if (successText) setStatusText(statusSelector, successText, "success");
  return body;
}

async function retrySourceIds(sourceIds, statusSelector, successLabel, extraPayload = {}) {
  if (extraPayload.failed_only) {
    const failedRows = (state.sourceConfig.sources || []).filter((source) => isFailedSource(source));
    if (!failedRows.length) {
      setStatusText(statusSelector, "当前没有失败信源需要重试", "success");
      return null;
    }
  } else if (!sourceIds.length) {
    setStatusText(statusSelector, "请先选中至少一个信源", "error");
    return null;
  }
  setStatusText(statusSelector, "正在慢慢重试抓取…", "loading");
  const { res, body } = await apiJson("/api/config/sources/retry", {
    source_ids: sourceIds,
    with_web: true,
    ...extraPayload,
  });
  if (!res.ok || body.ok === false) {
    setStatusText(statusSelector, body.error || `HTTP ${res.status}`, "error");
    return null;
  }
  setSourceConfig(body);
  await reloadIndex().catch(() => {});
  const okCount = (body.success_source_ids || []).length;
  const failCount = (body.failed_sources || []).length;
  setStatusText(statusSelector, `${successLabel}：成功 ${okCount}，失败 ${failCount}`, failCount ? "error" : "success");
  return body;
}

function renderSourceConfigList() {
  const wrap = $("#source-config-list");
  if (!wrap) return;
  if ($("#source-filter-search")) $("#source-filter-search").value = state.sourceFilterSearch;
  if ($("#source-filter-status")) $("#source-filter-status").value = state.sourceFilterStatus;
  wrap.innerHTML = "";
  const allRows = state.sourceConfig.sources || [];
  const rows = getFilteredSourceRows();
  const visibleSummary = $("#source-visible-summary");
  if (visibleSummary) visibleSummary.textContent = `${rows.length} / ${allRows.length}`;
  const selectionSummary = $("#source-selection-summary");
  if (selectionSummary) selectionSummary.textContent = `已选 ${state.selectedSourceIds.size} 项`;
  if (!rows.length) {
    wrap.appendChild(h("div", { class: "profile-empty" }, allRows.length ? "当前筛选下没有匹配信源" : "还没有配置可管理的信源"));
    return;
  }
  for (const source of rows) {
    const config = source.config || {};
    const runtime = sourceRuntime(source);
    const statusMeta = sourceStatusMeta(source);
    const selected = state.selectedSourceIds.has(source.id);
    const meta = [
      source.type || "source",
      config.category || "未分组",
      source.enabled ? "启用中" : "已关闭",
    ].filter(Boolean).join(" · ");
    const runtimeLine = source.type === "rss"
      ? runtime.has_data
        ? `最近数据：${runtime.latest_item_date || "—"} · 共 ${runtime.item_days || 0} 天`
        : runtime.status === "paused"
          ? "这个 RSS 目前已暂停，不会继续自动抓取。"
          : "还没抓到数据，可以直接点“重试”再试一轮。"
      : "系统内置信源";
    wrap.appendChild(h("div", { class: `profile-card source-profile-card ${source.enabled ? "" : "source-card-disabled"} ${selected ? "selected" : ""}`.trim() }, [
      h("div", { class: "profile-card-topline" }, [
        h("label", { class: "profile-card-select checkbox-pill" }, [
          h("input", {
            type: "checkbox",
            checked: selected,
            onchange: (event) => {
              if (event.target.checked) state.selectedSourceIds.add(source.id);
              else state.selectedSourceIds.delete(source.id);
              renderSourceConfigList();
            },
          }),
          h("span", {}, "选择"),
        ]),
        h("div", { class: "profile-card-main" }, [
          h("strong", {}, source.name || source.id),
          h("div", { class: "profile-card-meta" }, meta),
          config.feed_url ? h("div", { class: "profile-card-meta source-url" }, config.feed_url) : null,
          h("div", { class: "source-runtime-line" }, [
            h("span", { class: `source-status-badge ${statusMeta.className}` }, statusMeta.label),
            " ",
            h("strong", {}, runtimeLine),
            runtime.is_local_feed ? " · 本地源" : "",
          ]),
        ]),
      ]),
      h("div", { class: "profile-card-actions" }, [
        h("button", { class: "mini-btn", onclick: () => fillSourceForm(source) }, "编辑"),
        source.type === "rss"
          ? h("button", {
              class: "mini-btn",
              onclick: async () => {
                setStatusText("#source-bulk-edit-status", source.enabled ? "正在暂停…" : "正在启用…", "loading");
                await bulkUpdateSources(
                  { source_ids: [source.id], enabled: !source.enabled, with_web: true },
                  "#source-bulk-edit-status",
                  source.enabled ? "已暂停 1 个信源" : "已启用 1 个信源",
                );
              },
            }, source.enabled ? "暂停" : "启用")
          : null,
        source.type === "rss"
          ? h("button", {
              class: "mini-btn",
              onclick: async () => retrySourceIds([source.id], "#source-bulk-edit-status", `已重试 ${source.name || source.id}`),
            }, "重试")
          : null,
        h("button", {
          class: "mini-btn danger",
          onclick: async () => {
            const res = await fetch(`/api/config/sources/${encodeURIComponent(source.id)}`, { method: "DELETE" });
            const body = await res.json();
            setSourceConfig(body);
            fillSourceForm();
            state.selectedSourceIds.delete(source.id);
            await reloadIndex().catch(() => {});
          },
        }, "删除"),
      ]),
    ]));
  }
}

function parseBulkSourceLines(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function syncCategoryEmojiByLabel(labelValue, emojiSelector) {
  const category = String(labelValue || "").trim();
  const matched = (state.sourceConfig.categories || []).find((row) => row.label === category);
  const emojiInput = $(emojiSelector);
  if (matched && emojiInput && !emojiInput.value.trim()) {
    emojiInput.value = matched.emoji || "";
  }
}

function wireSourceConfigForm() {
  $("#source-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const payload = sourcePayload();
    if (!payload.name || !payload.config.feed_url) {
      setStatusText("#source-config-status", "请至少填名称和 RSS 地址", "error");
      return;
    }
    setStatusText("#source-config-status", "保存中…", "loading");
    const body = await saveSourceAndRefresh(payload, "#source-config-status", "已保存信源配置");
    if (!body) return;
    fillSourceForm();
  });

  $("#source-reset")?.addEventListener("click", () => fillSourceForm());

  $("#source-bulk-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const text = $("#source-bulk-import")?.value || "";
    if (!parseBulkSourceLines(text).length) {
      setStatusText("#source-bulk-status", "请先粘贴要导入的 RSS 列表", "error");
      return;
    }
    setStatusText("#source-bulk-status", "批量导入中…", "loading");
    const { res, body } = await apiJson("/api/config/rss/import", {
      text,
      default_category: $("#source-bulk-default-category")?.value.trim() || "",
      default_emoji: $("#source-bulk-default-emoji")?.value.trim() || "",
      sync: Boolean($("#source-bulk-sync")?.checked),
      with_web: true,
    });
    if (!res.ok || body.ok === false) {
      setStatusText("#source-bulk-status", body.error || `HTTP ${res.status}`, "error");
      return;
    }
    setSourceConfig(body);
    $("#source-bulk-import").value = "";
    await reloadIndex().catch(() => {});
    const count = (body.created_source_ids || []).length;
    setStatusText("#source-bulk-status", `已导入 ${count} 个信源`, "success");
  });

  $("#source-category")?.addEventListener("change", () => syncCategoryEmojiByLabel($("#source-category")?.value, "#source-category-emoji"));
  $("#source-bulk-category")?.addEventListener("change", () => syncCategoryEmojiByLabel($("#source-bulk-category")?.value, "#source-bulk-category-emoji"));
  $("#source-bulk-default-category")?.addEventListener("change", () => syncCategoryEmojiByLabel($("#source-bulk-default-category")?.value, "#source-bulk-default-emoji"));

  $("#source-filter-search")?.addEventListener("input", (e) => {
    state.sourceFilterSearch = e.target.value || "";
    renderSourceConfigList();
  });
  $("#source-filter-category")?.addEventListener("change", (e) => {
    state.sourceFilterCategory = e.target.value || "";
    renderSourceConfigList();
  });
  $("#source-filter-status")?.addEventListener("change", (e) => {
    state.sourceFilterStatus = e.target.value || "all";
    renderSourceConfigList();
  });

  $("#source-select-visible")?.addEventListener("click", () => {
    for (const row of getFilteredSourceRows()) state.selectedSourceIds.add(row.id);
    renderSourceConfigList();
  });
  $("#source-clear-selection")?.addEventListener("click", () => {
    state.selectedSourceIds.clear();
    renderSourceConfigList();
  });

  $("#source-bulk-apply-category")?.addEventListener("click", async () => {
    const selected = [...state.selectedSourceIds];
    const category = $("#source-bulk-category")?.value.trim() || "";
    if (!selected.length) {
      setStatusText("#source-bulk-edit-status", "请先选中要改分类的信源", "error");
      return;
    }
    if (!category) {
      setStatusText("#source-bulk-edit-status", "请先填一个目标分类", "error");
      return;
    }
    setStatusText("#source-bulk-edit-status", "批量改分类中…", "loading");
    await bulkUpdateSources({
      source_ids: selected,
      category,
      category_emoji: $("#source-bulk-category-emoji")?.value.trim() || "",
      with_web: true,
    }, "#source-bulk-edit-status", `已更新 ${selected.length} 个信源分类`);
  });

  $("#source-bulk-enable")?.addEventListener("click", async () => {
    const selected = [...state.selectedSourceIds];
    if (!selected.length) {
      setStatusText("#source-bulk-edit-status", "请先选中要启用的信源", "error");
      return;
    }
    setStatusText("#source-bulk-edit-status", "批量启用中…", "loading");
    await bulkUpdateSources({ source_ids: selected, enabled: true, with_web: true }, "#source-bulk-edit-status", `已启用 ${selected.length} 个信源`);
  });

  $("#source-bulk-disable")?.addEventListener("click", async () => {
    const selected = [...state.selectedSourceIds];
    if (!selected.length) {
      setStatusText("#source-bulk-edit-status", "请先选中要暂停的信源", "error");
      return;
    }
    setStatusText("#source-bulk-edit-status", "批量暂停中…", "loading");
    await bulkUpdateSources({ source_ids: selected, enabled: false, with_web: true }, "#source-bulk-edit-status", `已暂停 ${selected.length} 个信源`);
  });

  $("#source-bulk-retry-selected")?.addEventListener("click", async () => {
    await retrySourceIds([...state.selectedSourceIds], "#source-bulk-edit-status", "已重试所选信源");
  });

  $("#source-bulk-retry-failed")?.addEventListener("click", async () => {
    await retrySourceIds([], "#source-bulk-edit-status", "已重试失败信源", { failed_only: true });
  });
}

function setStatusText(selector, text = "", kind = "") {
  const el = $(selector);
  if (!el) return;
  el.textContent = text;
  el.setAttribute("data-state", kind || "idle");
}

function syncBackfillMode(prefix = "backfill") {
  const mode = $(`#${prefix}-mode`)?.value || "single";
  const single = $(`#${prefix}-single-group`);
  const range = $(`#${prefix}-range-group`);
  if (single) single.hidden = mode !== "single";
  if (range) range.hidden = mode !== "range";
}

function applyBackfillPresetFor(prefix, preset) {
  const newest = getNewestDate();
  const singleField = $(`#${prefix}-single-date`) || (prefix === "digest-backfill" ? $("#digest-run-date") : null);
  const startField = $(`#${prefix}-start-date`);
  const endField = $(`#${prefix}-end-date`);
  const modeField = $(`#${prefix}-mode`);
  const statusSelector = prefix === "backfill"
    ? "#backfill-status"
    : prefix === "tagging-backfill"
      ? "#tagging-backfill-status"
      : "#digest-run-status";
  if (preset === "today") {
    if (modeField) modeField.value = "single";
    if (singleField) singleField.value = newest;
  } else if (preset === "yesterday") {
    if (modeField) modeField.value = "single";
    if (singleField) singleField.value = addDays(newest, -1);
  } else if (preset === "last7") {
    if (modeField) modeField.value = "range";
    if (startField) startField.value = addDays(newest, -6);
    if (endField) endField.value = newest;
  } else if (preset === "month") {
    if (modeField) modeField.value = "range";
    const start = newest ? `${newest.slice(0, 8)}01` : "";
    if (startField) startField.value = start;
    if (endField) endField.value = newest;
  }
  syncBackfillMode(prefix);
  setStatusText(statusSelector, "");
}

function collectBackfillDates(prefix = "backfill") {
  const mode = $(`#${prefix}-mode`)?.value || "single";
  if (mode === "single") {
    const date = $(`#${prefix}-single-date`)?.value || (prefix === "digest-backfill" ? ($("#digest-run-date")?.value || "") : "");
    return date ? [date] : [];
  }
  const start = $(`#${prefix}-start-date`)?.value || "";
  const end = $(`#${prefix}-end-date`)?.value || "";
  return generateDateRange(start, end);
}

function wireBackfillForm() {
  const queueFront = async ({ task, prefix, forceSelector, statusSelector, fallbackText }) => {
    const dates = collectBackfillDates(prefix);
    if (!dates.length) {
      setStatusText(statusSelector, "请选择有效日期", "error");
      return;
    }
    const force = Boolean($(forceSelector)?.checked);
    await runQueueAction(task, { action: "prioritize", dates, kind: "backfill", force }, `已插队：${dates.join(", ")}${force ? "（force）" : ""}`);
  };

  $("#backfill-mode")?.addEventListener("change", () => syncBackfillMode("backfill"));
  $("#tagging-backfill-mode")?.addEventListener("change", () => syncBackfillMode("tagging-backfill"));
  $("#digest-backfill-mode")?.addEventListener("change", () => syncBackfillMode("digest-backfill"));
  ["#backfill-presets", "#tagging-backfill-presets", "#digest-backfill-presets"].forEach((selector) => $(selector)?.addEventListener("click", (e) => {
    const button = e.target.closest("[data-backfill-preset]");
    if (!button) return;
    applyBackfillPresetFor(button.getAttribute("data-backfill-target") || "backfill", button.getAttribute("data-backfill-preset"));
  }));

  $("#backfill-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const dates = collectBackfillDates("backfill");
    if (!dates.length) {
      setStatusText("#backfill-status", "请选择有效日期", "error");
      return;
    }
    setStatusText("#backfill-status", "已提交补跑任务…", "loading");
    const { res, body } = await apiJson("/api/scoring/backfill", { dates, force: Boolean($("#backfill-force-check")?.checked) });
    if (!res.ok || body.ok === false) {
      setStatusText("#backfill-status", humanizeBackfillReason(body.error) || `HTTP ${res.status}`, "error");
      loadScoringStatus().catch(() => {});
      return;
    }
    setStatusText("#backfill-status", `已排队：${dates.join(", ")}${$("#backfill-force-check")?.checked ? "（force）" : ""}`, "success");
    loadScoringStatus().catch(() => {});
  });
  $("#backfill-prioritize")?.addEventListener("click", async () => {
    await queueFront({ task: "scoring", prefix: "backfill", forceSelector: "#backfill-force-check", statusSelector: "#backfill-status" });
  });

  $("#tagging-backfill-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const dates = collectBackfillDates("tagging-backfill");
    if (!dates.length) {
      setStatusText("#tagging-backfill-status", "请选择有效日期", "error");
      return;
    }
    setStatusText("#tagging-backfill-status", "已提交标签补跑任务…", "loading");
    const { res, body } = await apiJson("/api/tagging/backfill", { dates, force: Boolean($("#tagging-backfill-force-check")?.checked) });
    if (!res.ok || body.ok === false) {
      setStatusText("#tagging-backfill-status", humanizeBackfillReason(body.error) || body.error || `HTTP ${res.status}`, "error");
      loadTaggingStatus().catch(() => {});
      return;
    }
    setStatusText("#tagging-backfill-status", `已排队：${dates.join(", ")}${$("#tagging-backfill-force-check")?.checked ? "（force）" : ""}`, "success");
    loadTaggingStatus().catch(() => {});
  });
  $("#tagging-backfill-prioritize")?.addEventListener("click", async () => {
    await queueFront({ task: "tagging", prefix: "tagging-backfill", forceSelector: "#tagging-backfill-force-check", statusSelector: "#tagging-backfill-status" });
  });

  $("#digest-run-form")?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const dates = collectBackfillDates("digest-backfill");
    if (!dates.length) {
      setStatusText("#digest-run-status", "请选择有效摘要日期", "error");
      return;
    }
    setStatusText("#digest-run-status", "已提交摘要生成…", "loading");
    const force = Boolean($("#digest-run-force-check")?.checked);
    const payload = dates.length === 1 ? { date: dates[0], force } : { dates, force };
    const { res, body } = await apiJson("/api/digest/run", payload);
    if (!res.ok || body.ok === false) {
      setStatusText("#digest-run-status", body.error || `HTTP ${res.status}`, "error");
      loadDigestStatus().catch(() => {});
      return;
    }
    setStatusText("#digest-run-status", `已排队：${dates.join(", ")}${force ? "（force）" : ""}`, "success");
    loadDigestStatus().catch(() => {});
  });
  $("#digest-run-prioritize")?.addEventListener("click", async () => {
    await queueFront({ task: "digest", prefix: "digest-backfill", forceSelector: "#digest-run-force-check", statusSelector: "#digest-run-status" });
  });

  syncBackfillMode("backfill");
  syncBackfillMode("tagging-backfill");
  syncBackfillMode("digest-backfill");
}

function seedBackfillInputs() {
  const newest = getNewestDate();
  if ($("#backfill-single-date") && !$("#backfill-single-date").value) $("#backfill-single-date").value = newest;
  if ($("#backfill-start-date") && !$("#backfill-start-date").value) $("#backfill-start-date").value = addDays(newest, -6);
  if ($("#backfill-end-date") && !$("#backfill-end-date").value) $("#backfill-end-date").value = newest;
  if ($("#tagging-backfill-single-date") && !$("#tagging-backfill-single-date").value) $("#tagging-backfill-single-date").value = newest;
  if ($("#tagging-backfill-start-date") && !$("#tagging-backfill-start-date").value) $("#tagging-backfill-start-date").value = addDays(newest, -6);
  if ($("#tagging-backfill-end-date") && !$("#tagging-backfill-end-date").value) $("#tagging-backfill-end-date").value = newest;
  if ($("#digest-run-date") && !$("#digest-run-date").value) $("#digest-run-date").value = addDays(newest, -1);
  if ($("#digest-backfill-start-date") && !$("#digest-backfill-start-date").value) $("#digest-backfill-start-date").value = addDays(newest, -6);
  if ($("#digest-backfill-end-date") && !$("#digest-backfill-end-date").value) $("#digest-backfill-end-date").value = newest;
  syncBackfillMode("backfill");
  syncBackfillMode("tagging-backfill");
  syncBackfillMode("digest-backfill");
}

function renderProfileCards({ mount, profiles, activeProfile, onActivate, onEdit, onDelete }) {
  const wrap = $(mount);
  if (!wrap) return;
  wrap.innerHTML = "";
  if (!profiles?.length) {
    wrap.appendChild(h("div", { class: "profile-empty" }, "还没有模型配置"));
    return;
  }
  for (const profile of profiles) {
    wrap.appendChild(h("div", { class: "profile-card" + (profile.id === activeProfile ? " active" : "") }, [
      h("div", { class: "profile-card-main" }, [
        h("strong", {}, profile.name || profile.id),
        h("div", { class: "profile-card-meta" }, `${profile.provider || "provider"} · ${profile.model || "model"} · batch ${profile.batch_size || 1}`),
      ]),
      h("div", { class: "profile-card-actions" }, [
        h("button", { class: "mini-btn", onclick: () => onActivate(profile) }, profile.id === activeProfile ? "当前使用" : "设为当前"),
        h("button", { class: "mini-btn", onclick: () => onEdit(profile) }, "编辑"),
        h("button", { class: "mini-btn danger", onclick: () => onDelete(profile) }, "删除"),
      ]),
    ]));
  }
}

function renderProfileList() {
  renderProfileCards({
    mount: "#profile-list",
    profiles: state.scoringConfig.profiles || [],
    activeProfile: state.scoringConfig.active_profile,
    onActivate: async (profile) => {
      const { body } = await apiJson("/api/config/scoring", { active_profile: profile.id });
      setScoringConfig(body.scoring);
      loadScoringStatus().catch(() => {});
    },
    onEdit: (profile) => fillProfileForm(profile),
    onDelete: async (profile) => {
      const res = await fetch(`/api/config/scoring/profiles/${encodeURIComponent(profile.id)}`, { method: "DELETE" });
      const body = await res.json();
      setScoringConfig(body.scoring);
      fillProfileForm();
      loadScoringStatus().catch(() => {});
    },
  });
}

function renderTaggingProfileList() {
  renderProfileCards({
    mount: "#tagging-profile-list",
    profiles: state.taggingConfig.profiles || [],
    activeProfile: state.taggingConfig.active_profile,
    onActivate: async (profile) => {
      const { body } = await apiJson("/api/config/tagging", { active_profile: profile.id });
      setTaggingConfig(body.tagging);
      loadTaggingStatus().catch(() => {});
    },
    onEdit: (profile) => fillTaggingProfileForm(profile),
    onDelete: async (profile) => {
      const res = await fetch(`/api/config/tagging/profiles/${encodeURIComponent(profile.id)}`, { method: "DELETE" });
      const body = await res.json();
      setTaggingConfig(body.tagging);
      fillTaggingProfileForm();
      loadTaggingStatus().catch(() => {});
    },
  });
}

function renderDigestProfileList() {
  renderProfileCards({
    mount: "#digest-profile-list",
    profiles: state.digestConfig.profiles || [],
    activeProfile: state.digestConfig.active_profile,
    onActivate: async (profile) => {
      const { body } = await apiJson("/api/config/daily-digest", { active_profile: profile.id });
      setDigestConfig(body.daily_digest);
      loadDigestStatus().catch(() => {});
    },
    onEdit: (profile) => fillDigestProfileForm(profile),
    onDelete: async (profile) => {
      const res = await fetch(`/api/config/daily-digest/profiles/${encodeURIComponent(profile.id)}`, { method: "DELETE" });
      const body = await res.json();
      setDigestConfig(body.daily_digest);
      fillDigestProfileForm();
      loadDigestStatus().catch(() => {});
    },
  });
}

function fillProfileForm(profile = null) {
  $("#profile-id").value = profile?.id || "";
  $("#profile-name").value = profile?.name || "";
  setSelectValue("#profile-provider", profile?.provider || "openai_compatible", "openai_compatible");
  $("#profile-base-url").value = profile?.base_url || "";
  $("#profile-api-key").value = profile?.api_key || "";
  $("#profile-model").value = profile?.model || "";
  setSelectValue("#profile-batch-size", profile?.batch_size || 1, "1");
}

function fillTaggingProfileForm(profile = null) {
  $("#tagging-profile-id").value = profile?.id || "";
  $("#tagging-profile-name").value = profile?.name || "";
  setSelectValue("#tagging-profile-provider", profile?.provider || "openai_compatible", "openai_compatible");
  $("#tagging-profile-base-url").value = profile?.base_url || "";
  $("#tagging-profile-api-key").value = profile?.api_key || "";
  $("#tagging-profile-model").value = profile?.model || "";
  setSelectValue("#tagging-profile-batch-size", profile?.batch_size || 1, "1");
}

function fillDigestProfileForm(profile = null) {
  $("#digest-profile-id").value = profile?.id || "";
  $("#digest-profile-name").value = profile?.name || "";
  setSelectValue("#digest-profile-provider", profile?.provider || "openai_compatible", "openai_compatible");
  $("#digest-profile-base-url").value = profile?.base_url || "";
  $("#digest-profile-api-key").value = profile?.api_key || "";
  $("#digest-profile-model").value = profile?.model || "";
  setSelectValue("#digest-profile-batch-size", profile?.batch_size || 1, "1");
}

function syncPrimaryViews() {
  const digestVisible = state.activeView === "digest";
  $("#digest-view")?.toggleAttribute("hidden", !digestVisible);
  if (digestVisible) closeWorkbenchPopover();
  $("#timeline")?.classList.toggle("digest-mode", digestVisible);
  $("#digest-btn")?.setAttribute("data-active", digestVisible ? "true" : "false");
  if (digestVisible) {
    loadDigestIndex().then(() => renderDigestView()).catch(() => {
      const content = $("#digest-content");
      if (content) content.innerHTML = '<div class="runtime-empty">暂无 digest 数据。</div>';
    });
  }
}

function toggleDigestView(forceView = null) {
  state.activeView = forceView || (state.activeView === "feed" ? "digest" : "feed");
  syncPrimaryViews();
}

function wireTopbar() {
  $("#refresh-btn").addEventListener("click", refreshLatestWindow);
  $("#theme-btn").addEventListener("click", () => {
    const order = ["auto", "light", "dark"];
    const cur = state.settings.theme;
    state.settings.theme = order[(order.indexOf(cur) + 1) % order.length];
    saveSettings();
    applyTheme();
    syncSettingsForm();
  });
  $("#workbench-toggle-btn")?.addEventListener("click", (event) => {
    event.stopPropagation();
    setWorkbenchExpanded(!state.workbenchExpanded);
  });
  $("#settings-btn").addEventListener("click", () => openSettingsModal());
  $("#settings-close-btn")?.addEventListener("click", closeSettingsModal);
  $("#settings-backdrop")?.addEventListener("click", closeSettingsModal);
  $("#private-unlock-btn")?.addEventListener("click", openPrivateUnlockModal);
  $("#private-unlock-close")?.addEventListener("click", closePrivateUnlockModal);
  $("#private-unlock-cancel")?.addEventListener("click", closePrivateUnlockModal);
  $("#private-unlock-backdrop")?.addEventListener("click", closePrivateUnlockModal);
  $("#private-unlock-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    unlockPrivateBundle($("#private-password")?.value || "");
  });
  $("#latest-btn").addEventListener("click", () => { state.activeView = "feed"; syncPrimaryViews(); window.scrollTo({ top: 0, behavior: "smooth" }); });
  $("#digest-btn")?.addEventListener("click", () => toggleDigestView());
  $("#digest-refresh-btn")?.addEventListener("click", () => refreshDigestCurrentDate());
  $("#digest-read-mode-btn")?.addEventListener("click", () => toggleDigestReadMode());
  $("#digest-date-select")?.addEventListener("change", (e) => {
    state.selectedDigestDate = e.target.value || null;
    renderDigestView();
  });
  $("#digest-date-prev-btn")?.addEventListener("click", () => shiftDigestDate(1));
  $("#digest-date-next-btn")?.addEventListener("click", () => shiftDigestDate(-1));
  $("#digest-date-input")?.addEventListener("change", (e) => {
    const sourceDate = resolveDigestSourceDate(e.target.value, state.activeDigestTab);
    if (!sourceDate) {
      setRefreshStatus("error", "这一天暂时没有日报，可以换相邻日期或点“刷新摘要”");
      renderDigestDateSelect();
      window.setTimeout(() => setRefreshStatus(), 3500);
      return;
    }
    selectDigestDate(sourceDate);
  });
  $("#search").addEventListener("input", (e) => {
    state.search = e.target.value.trim().toLowerCase();
    renderTimeline();
  });
  document.addEventListener("click", (event) => {
    const popover = $("#workbench-popover");
    const button = $("#workbench-toggle-btn");
    if (!state.workbenchExpanded || !popover || !button) return;
    if (popover.contains(event.target) || button.contains(event.target)) return;
    closeWorkbenchPopover();
  });
}

function wireSourceAlert() {
  $("#sync-alert-refresh-btn")?.addEventListener("click", () => {
    loadSourceStatus(true).catch((err) => setRefreshStatus("error", err.message || "同步状态刷新失败"));
  });
  $("#sync-reauth-btn")?.addEventListener("click", async () => {
    const authWindow = window.open("", "_blank", "noopener");
    try {
      const { res, body } = await apiJson("/api/source/reauthorize", { auto_resume: true });
      if (!res.ok || body.ok === false) {
        if (authWindow && !authWindow.closed) authWindow.close();
        setRefreshStatus("error", body.error || `HTTP ${res.status}`);
        return;
      }
      const session = body.session || {};
      state.sourceStatus.auth_session = session;
      renderSourceAlert();
      if (session.verification_url) {
        if (authWindow && !authWindow.closed) {
          authWindow.location.replace(session.verification_url);
        } else {
          window.open(session.verification_url, "_blank", "noopener");
        }
      } else if (authWindow && !authWindow.closed) {
        authWindow.close();
      }
      loadSourceStatus(true).catch(() => {});
    } catch (err) {
      if (authWindow && !authWindow.closed) authWindow.close();
      setRefreshStatus("error", err?.message || "重新授权失败");
    }
  });
  $("#sync-catchup-btn")?.addEventListener("click", async () => {
    setRefreshStatus("loading", "补跑中…");
    const { res, body } = await apiJson("/api/source/catch-up", {});
    if (!res.ok || body.ok === false) {
      setRefreshStatus("error", body.error || `HTTP ${res.status}`);
      loadSourceStatus(true).catch(() => {});
      return;
    }
    for (const date of body.dates || []) state.loadedDays.delete(date);
    await reloadIndex();
    await Promise.all([
      loadSourceStatus(true).catch(() => {}),
      loadScoringStatus().catch(() => {}),
      loadTaggingStatus().catch(() => {}),
      loadDigestStatus().catch(() => {}),
      loadDigestIndex().catch(() => {}),
      loadUserStates().catch(() => {}),
    ]);
    await renderTimeline();
    renderSidebar();
    setRefreshStatus("success", (body.dates || []).length ? `已补跑 ${body.dates.join(", ")}` : "当前没有缺口");
    window.setTimeout(() => { if (!state.refreshing) setRefreshStatus(); }, 4500);
  });
}

function wireKeys() {
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isPrivateUnlockModalOpen()) {
      closePrivateUnlockModal();
      if (e.target.matches("input, textarea, select")) e.target.blur();
      return;
    }
    if (e.key === "Escape" && isSettingsModalOpen()) {
      closeSettingsModal();
      if (e.target.matches("input, textarea, select")) e.target.blur();
      return;
    }
    if (e.key === "Escape" && state.workbenchExpanded) {
      closeWorkbenchPopover();
      if (e.target.matches("input, textarea, select")) e.target.blur();
      return;
    }
    if (e.target.matches("input, textarea, select")) {
      if (e.key === "Escape") e.target.blur();
      return;
    }
    if (e.key === "/") { e.preventDefault(); $("#search").focus(); }
    if (e.key === "t" || e.key === "T") $("#theme-btn").click();
    if (e.key === "l" || e.key === "L") $("#latest-btn").click();
    if (e.key === "d" || e.key === "D") $("#digest-btn")?.click();
    if (e.key === "r" || e.key === "R") $("#refresh-btn").click();
    if (e.key === "f" || e.key === "F") $("#workbench-toggle-btn")?.click();
    if (e.key === ",") $("#settings-btn").click();
    if (state.activeView === "digest" && e.key === "ArrowLeft") { e.preventDefault(); shiftDigestDate(1); }
    if (state.activeView === "digest" && e.key === "ArrowRight") { e.preventDefault(); shiftDigestDate(-1); }
    if (e.key === "j" || e.key === "J") scrollDayOffset(1);
    if (e.key === "k" || e.key === "K") scrollDayOffset(-1);
  });
}
function scrollDayOffset(delta) {
  const days = $$(".day");
  const y = window.scrollY + 100;
  let idx = days.findIndex((d) => d.offsetTop > y);
  if (idx < 0) idx = days.length;
  const target = days[Math.max(0, Math.min(days.length - 1, idx + delta - 1))];
  if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
}

// ─────────────────────────── Sidebar render ──────────────────
function renderSidebar() {
  const dl = $("#date-list"); dl.innerHTML = "";
  for (const d of state.index.days) {
    dl.appendChild(h("div", {
      class: `date-item${state.selectedDate === d.date ? " active" : ""}`,
      "data-date": d.date,
      onclick: () => jumpToDay(d.date),
    }, [d.date, h("span", { class: "count" }, String(d.items))]));
  }
  syncSidebarDateState();

  const sf = $("#source-filters"); sf.innerHTML = "";
  const sourceNames = new Set();
  for (const day of state.index.days) {
    for (const s of Object.keys(day.sources || {})) sourceNames.add(s);
  }
  const srcs = [...sourceNames].sort();
  if (!srcs.length) state.index.sources.forEach(s => srcs.push(s.name));
  for (const name of srcs) {
    sf.appendChild(chip(name, state.activeSources.has(name), () => { toggle(state.activeSources, name); renderSidebar(); renderTimeline(); }));
  }
  $("#clear-sources")?.toggleAttribute("hidden", state.activeSources.size === 0);
  $("#clear-sources")?.toggleAttribute("disabled", state.activeSources.size === 0);

  const entityTotals = {};
  const topicTotals = {};
  for (const day of state.loadedDays.values()) {
    for (const item of day.items || []) {
      for (const tag of item.entity_tags || []) entityTotals[tag] = (entityTotals[tag] || 0) + 1;
      for (const tag of item.topic_tags || []) topicTotals[tag] = (topicTotals[tag] || 0) + 1;
    }
  }
  const entityWrap = $("#entity-tag-filters");
  if (entityWrap) {
    entityWrap.innerHTML = "";
    for (const [tag, count] of Object.entries(entityTotals).sort((a, b) => b[1] - a[1]).slice(0, 40)) {
      entityWrap.appendChild(chip(tag, state.activeEntityTags.has(tag), () => { toggle(state.activeEntityTags, tag); renderSidebar(); renderTimeline(); }, count));
    }
  }
  $("#clear-entity-tags")?.toggleAttribute("hidden", state.activeEntityTags.size === 0);
  $("#clear-entity-tags")?.toggleAttribute("disabled", state.activeEntityTags.size === 0);

  const topicWrap = $("#topic-tag-filters");
  if (topicWrap) {
    topicWrap.innerHTML = "";
    for (const [tag, count] of Object.entries(topicTotals).sort((a, b) => b[1] - a[1]).slice(0, 40)) {
      topicWrap.appendChild(chip(tag, state.activeTopicTags.has(tag), () => { toggle(state.activeTopicTags, tag); renderSidebar(); renderTimeline(); }, count));
    }
  }
  $("#clear-topic-tags")?.toggleAttribute("hidden", state.activeTopicTags.size === 0);
  $("#clear-topic-tags")?.toggleAttribute("disabled", state.activeTopicTags.size === 0);

  const itemStateFilters = $("#item-state-filters");
  if (itemStateFilters) {
    itemStateFilters.innerHTML = "";
    for (const itemState of ITEM_STATE_FILTERS) {
      itemStateFilters.appendChild(chip(itemState.label, state.activeItemStates.has(itemState.key), () => {
        toggle(state.activeItemStates, itemState.key); renderSidebar(); renderTimeline();
      }));
    }
  }
  $("#clear-item-state-filters")?.toggleAttribute("hidden", state.activeItemStates.size === 0);
  $("#clear-item-state-filters")?.toggleAttribute("disabled", state.activeItemStates.size === 0);

  const cf = $("#category-filters"); cf.innerHTML = "";
  const catTotals = {};
  for (const day of state.index.days) {
    for (const [k, v] of Object.entries(day.categories || {})) catTotals[k] = (catTotals[k] || 0) + v;
  }
  const cats = (state.index.categories || []).map(c => c.label);
  for (const cat of cats) {
    const n = catTotals[cat] || 0;
    cf.appendChild(chip(cat, state.activeCategories.has(cat), () => { toggle(state.activeCategories, cat); renderSidebar(); renderTimeline(); }, n));
  }

  const scoreFilters = $("#score-filters");
  if (scoreFilters) {
    scoreFilters.innerHTML = "";
    for (const label of SCORE_LABELS) {
      const active = label === "全部" ? state.activeScoreLabels.size === 0 : state.activeScoreLabels.has(label);
      scoreFilters.appendChild(chip(label, active, () => { toggleSetValue(state.activeScoreLabels, label); renderSidebar(); renderTimeline(); }));
    }
  }
  $("#clear-score-filters")?.toggleAttribute("hidden", state.activeScoreLabels.size === 0);
  $("#clear-score-filters")?.toggleAttribute("disabled", state.activeScoreLabels.size === 0);

  for (const dimension of SCORE_DIMENSIONS) {
    const mount = $(dimension.mount);
    const activeSet = state.activeLevels[dimension.key];
    if (!mount || !activeSet) continue;
    mount.innerHTML = "";
    for (const level of LEVEL_FILTERS) {
      const active = level === "全部" ? activeSet.size === 0 : activeSet.has(level);
      mount.appendChild(chip(level, active, () => { toggleSetValue(activeSet, level); renderSidebar(); renderTimeline(); }));
    }
    $(dimension.clear)?.toggleAttribute("hidden", activeSet.size === 0);
    $(dimension.clear)?.toggleAttribute("disabled", activeSet.size === 0);
  }

  const hasAnyFilters = Boolean(state.selectedDate) || state.activeCategories.size || state.activeSources.size || state.activeEntityTags.size || state.activeTopicTags.size || state.activeScoreLabels.size || state.activeItemStates.size || countDayQuickFilters(state.selectedDate || state.dayQuickFilters?.date) || Object.values(state.activeLevels).some((set) => set.size) || Boolean(state.search);
  $("#clear-all-filters")?.toggleAttribute("hidden", !hasAnyFilters);
  $("#clear-all-filters")?.toggleAttribute("disabled", !hasAnyFilters);
}

function chip(label, active, onClick, count) {
  const kids = [h("span", {}, label)];
  if (count != null) kids.push(h("span", { class: "count" }, String(count)));
  return h("button", { class: "chip" + (active ? " active" : ""), type: "button", onclick: onClick }, kids);
}
function toggle(set, v) { set.has(v) ? set.delete(v) : set.add(v); }
function setSingleFilterValue(set, value) {
  if (set.size === 1 && set.has(value)) {
    set.clear();
    return;
  }
  set.clear();
  set.add(value);
}
function resetTimelineWindow() {
  state.timelineWindowStartIndex = null;
  state.timelineWindowEndIndex = null;
}
function setTimelineWindowForDate(date) {
  const idx = state.index?.days?.findIndex((day) => day.date === date) ?? -1;
  if (idx < 0) {
    resetTimelineWindow();
    return false;
  }
  state.timelineWindowStartIndex = idx;
  state.timelineWindowEndIndex = idx;
  return true;
}
function getTimelineWindowIndexes() {
  const days = state.index?.days || [];
  if (!days.length) return null;
  if (state.selectedDate) {
    const selectedIndex = Math.max(0, days.findIndex((day) => day.date === state.selectedDate));
    const startIndex = Number.isInteger(state.timelineWindowStartIndex)
      ? Math.max(0, Math.min(state.timelineWindowStartIndex, days.length - 1))
      : selectedIndex;
    const endIndex = Number.isInteger(state.timelineWindowEndIndex)
      ? Math.max(startIndex, Math.min(state.timelineWindowEndIndex, days.length - 1))
      : selectedIndex;
    return { startIndex, endIndex };
  }
  if (Number.isInteger(state.timelineWindowStartIndex) && Number.isInteger(state.timelineWindowEndIndex)) {
    const startIndex = Math.max(0, Math.min(state.timelineWindowStartIndex, days.length - 1));
    const endIndex = Math.max(startIndex, Math.min(state.timelineWindowEndIndex, days.length - 1));
    return { startIndex, endIndex };
  }
  const endIndex = Math.max(0, Math.min(state.timelineCount, days.length) - 1);
  return { startIndex: 0, endIndex };
}
function getVisibleTimelineDays() {
  const indexes = getTimelineWindowIndexes();
  if (!indexes) return [];
  const { startIndex, endIndex } = indexes;
  return state.index.days.slice(startIndex, endIndex + 1);
}
function ensureDefaultFeedDate() {
  if (!state.selectedDate && state.index?.days?.length) {
    state.selectedDate = state.index.days[0].date;
    setTimelineWindowForDate(state.selectedDate);
  }
}
async function showContinuousTimeline() {
  state.selectedDate = null;
  resetTimelineWindow();
  clearDayQuickFilters();
  state.timelineCount = 1;
  renderSidebar();
  await renderTimeline();
}
function rerenderWorkbenchOnly() {
  if (!state.index?.days?.length) return;
  renderFeedWorkbench(getVisibleTimelineDays());
}
function setWorkbenchExpanded(expanded) {
  state.workbenchExpanded = Boolean(expanded);
  saveWorkbenchExpanded();
  rerenderWorkbenchOnly();
  syncWorkbenchToggleButton();
}
function closeWorkbenchPopover() {
  setWorkbenchExpanded(false);
}
function syncWorkbenchToggleButton(metaText = "") {
  const btn = $("#workbench-toggle-btn");
  const meta = $("#workbench-toggle-meta");
  if (btn) btn.setAttribute("aria-expanded", state.workbenchExpanded ? "true" : "false");
  if (meta && metaText) meta.textContent = metaText;
}

// ─────────────────────────── Timeline render ─────────────────
const MONTHS = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"];
const WEEKDAYS = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"];

async function renderTimeline() {
  const tl = $("#timeline");
  const days = getVisibleTimelineDays();

  // Keep the old timeline visible while fetching newly-required day files.
  await Promise.all(days.map((d) => ensureDay(d.date)));

  // Remove all but the empty-state container.
  $$(".day", tl).forEach(n => n.remove());
  $$(".load-more", tl).forEach(n => n.remove());

  renderFeedWorkbench(days);

  const windowIndexes = getTimelineWindowIndexes();
  if (state.selectedDate && windowIndexes?.startIndex > 0) {
    tl.appendChild(renderDateLoadBoundary("newer", windowIndexes.startIndex));
  }

  let totalVisible = 0;
  let renderedDays = 0;
  for (const dMeta of days) {
    const day = state.loadedDays.get(dMeta.date);
    const baseItems = filterGlobalItems(day.items);
    const items = applyDayQuickFilters(baseItems, dMeta.date);
    if (!items.length) {
      if (!state.selectedDate) continue;
      renderedDays += 1;
      tl.appendChild(renderEmptyDay(dMeta, "这个日期在当前筛选下没有条目。", baseItems));
      continue;
    }
    totalVisible += items.length;
    renderedDays += 1;
    tl.appendChild(renderDay(dMeta, day, items, baseItems));
  }

  $("#empty").hidden = renderedDays > 0 || totalVisible > 0;

  if (state.selectedDate && windowIndexes?.endIndex < state.index.days.length - 1) {
    tl.appendChild(renderDateLoadBoundary("older", windowIndexes.endIndex));
  } else if (!state.selectedDate && state.timelineCount < state.index.days.length) {
    tl.appendChild(renderDateLoadBoundary("older", state.timelineCount - 1));
  }

  syncSidebarDateState(days[0]?.date || "");
}

function renderDateLoadBoundary(direction, edgeIndex) {
  const isNewer = direction === "newer";
  const nextIndex = isNewer ? Math.max(0, edgeIndex - 1) : Math.min(state.index.days.length - 1, edgeIndex + 1);
  const nextDay = state.index.days[nextIndex];
  const label = isNewer ? "LOAD NEWER EDITIONS" : "LOAD EARLIER EDITIONS";
  const hint = nextDay ? `${nextDay.date} · ${nextDay.items} 条` : "";
  const button = h("button", {
    onclick: async () => loadAdjacentTimelineDays(direction),
  }, label);
  const node = h("div", {
    class: `load-more timeline-boundary timeline-boundary-${direction}`,
    "data-timeline-boundary": direction,
  }, [button, hint ? h("div", { class: "load-more-hint" }, hint) : null]);
  observeAutoLoadMore(node, () => loadAdjacentTimelineDays(direction), AUTO_DAY_LOAD_ROOT_MARGIN, {
    scrollDirection: isNewer ? "up" : "down",
    notBefore: state.timelineAutoLoadDisabledUntil,
  });
  return node;
}

async function loadAdjacentTimelineDays(direction) {
  if (state.timelineAutoLoading || !state.index?.days?.length) return;
  state.timelineAutoLoading = true;
  let anchorDate = "";
  try {
    if (state.selectedDate) {
      const indexes = getTimelineWindowIndexes();
      if (!indexes) return;
      if (direction === "newer") {
        const nextStartIndex = Math.max(0, indexes.startIndex - TIMELINE_DAY_BATCH_COUNT);
        anchorDate = state.index.days[nextStartIndex]?.date || "";
        state.timelineWindowStartIndex = nextStartIndex;
      } else {
        state.timelineWindowEndIndex = Math.min(state.index.days.length - 1, indexes.endIndex + TIMELINE_DAY_BATCH_COUNT);
      }
    } else if (direction === "older") {
      state.timelineCount = Math.min(state.index.days.length, state.timelineCount + TIMELINE_DAY_BATCH_COUNT);
    }
    await renderTimeline();
    if (direction === "newer" && anchorDate) scrollToDayBottom(anchorDate);
  } finally {
    window.setTimeout(() => { state.timelineAutoLoading = false; }, 120);
  }
}

function scrollToDayBottom(date) {
  const el = [...$$(".day")].find((node) => node.getAttribute("data-date") === date);
  if (!el) return;
  const top = Math.max(0, el.getBoundingClientRect().top + window.scrollY + el.offsetHeight - window.innerHeight + 80);
  window.scrollTo({ top, behavior: "auto" });
}

function renderFeedWorkbench(days) {
  const shell = $("#workbench-popover");
  const mount = $("#workbench-popover-content");
  if (!mount || !shell) return;
  mount.innerHTML = "";

  if (!state.index?.days?.length || state.activeView !== "feed") {
    shell.hidden = true;
    syncWorkbenchToggleButton("日期 / 分组");
    return;
  }
  shell.hidden = !state.workbenchExpanded;

  const focusMeta = state.selectedDate
    ? state.index.days.find((day) => day.date === state.selectedDate) || days[0] || state.index.days[0]
    : days[0] || state.index.days[0];
  if (!focusMeta) {
    shell.hidden = true;
    return;
  }

  const focusDay = state.loadedDays.get(focusMeta.date) || { items: [] };
  const focusItems = filterItems(focusDay.items || [], focusMeta.date);
  const quickDates = getWorkbenchDates(focusMeta.date);
  const quickFilters = getDayQuickFilters(focusMeta.date);
  const quickFilterCount = countDayQuickFilters(focusMeta.date);
  const totalFilters = [
    ...state.activeCategories,
    ...state.activeSources,
    ...state.activeEntityTags,
    ...state.activeTopicTags,
    ...state.activeScoreLabels,
    ...state.activeItemStates,
  ].length + Object.values(state.activeLevels).reduce((sum, set) => sum + set.size, 0) + (state.search ? 1 : 0) + quickFilterCount;
  const summaryBits = [
    `${focusItems.length}/${focusMeta.items} 条`,
    state.settings.group === "category" ? "按分类" : state.settings.group === "source" ? "按信源" : "顺序看",
    state.selectedDate ? "单日阅读" : "多日浏览",
    state.activeCategories.size || state.activeSources.size || state.activeScoreLabels.size ? "左侧筛选生效中" : null,
    quickFilters.source ? `今日来源：${quickFilters.source}` : null,
    quickFilters.score ? `今日优先级：${quickFilters.score}` : null,
    totalFilters ? `${totalFilters} 个条件` : "无额外条件",
  ].filter(Boolean);
  syncWorkbenchToggleButton(`${focusMeta.date.slice(5)} / ${state.settings.group === "category" ? "分类" : state.settings.group === "source" ? "信源" : "顺序"}${totalFilters ? ` / ${totalFilters}条件` : ""}`);

  mount.appendChild(h("div", { class: "feed-workbench-shell is-expanded" }, [
    h("div", { class: "feed-workbench-head" }, [
      h("div", { class: "feed-workbench-copy" }, [
        h("div", { class: "panel-kicker" }, "Reading Controls"),
        h("h2", { class: "feed-workbench-title" }, state.selectedDate ? `${focusMeta.date} · 单日阅读` : `${focusMeta.date} · 当前聚焦日`),
        h("p", { class: "feed-workbench-subtitle" }, [
          `当前看到 ${focusItems.length} / ${focusMeta.items} 条`,
          state.settings.group === "category" ? " · 按分类浏览" : state.settings.group === "source" ? " · 按信源浏览" : " · 按时间顺序",
          " · 分类跳转和当天快筛放在日期栏；全局筛选放在左侧栏",
        ].join("")),
      ]),
      h("div", { class: "feed-workbench-actions" }, [
        h("button", {
          class: `ghost-chip ${state.selectedDate ? "" : "is-muted"}`.trim(),
          type: "button",
          onclick: async () => {
            if (state.selectedDate) {
              await showContinuousTimeline();
            } else {
              state.selectedDate = focusMeta.date;
              setTimelineWindowForDate(focusMeta.date);
              renderSidebar();
              await renderTimeline();
            }
          },
        }, state.selectedDate ? "返回多天视图" : "锁定当天"),
        h("button", {
          class: `ghost-chip ${totalFilters ? "" : "is-muted"}`.trim(),
          type: "button",
          onclick: () => {
            clearAllFilters();
            renderSidebar();
            renderTimeline();
          },
        }, "清空全部条件"),
      ]),
    ]),
    h("div", { class: "feed-workbench-summary" }, summaryBits.map((bit, index) =>
      h("span", { class: `feed-workbench-summary-chip ${index === 0 ? "is-strong" : ""}`.trim() }, bit)
    )),
    h("div", { class: "feed-workbench-panel" }, [
    h("div", { class: "feed-workbench-row" }, [
      h("div", { class: "feed-workbench-row-label" }, "切日期"),
      h("div", { class: "feed-workbench-strip" }, quickDates.map((day) => h("button", {
        class: `workbench-chip workbench-date-chip ${(state.selectedDate || focusMeta.date) === day.date ? "active" : ""}`,
        type: "button",
        onclick: () => jumpToDay(day.date),
      }, [
        h("span", { class: "workbench-date-main" }, day.date.slice(5)),
        h("span", { class: "count" }, String(day.items)),
      ]))),
    ]),
    h("div", { class: "feed-workbench-row feed-workbench-row-tight" }, [
      h("div", { class: "feed-workbench-row-label" }, "分组"),
      h("div", { class: "feed-workbench-seg" }, [
        renderWorkbenchModeButton("category", "按分类"),
        renderWorkbenchModeButton("source", "按信源"),
        renderWorkbenchModeButton("flat", "顺序看"),
      ]),
    ]),
    ]),
  ]));
}

function getWorkbenchDates(activeDate) {
  const days = state.index?.days || [];
  if (!days.length) return [];
  const activeIndex = Math.max(days.findIndex((day) => day.date === activeDate), 0);
  const start = Math.max(0, activeIndex - 2);
  const end = Math.min(days.length, start + 7);
  return days.slice(start, end);
}

function renderWorkbenchModeButton(mode, label) {
  return h("button", {
    class: `workbench-mode-btn ${state.settings.group === mode ? "active" : ""}`,
    type: "button",
    "aria-pressed": state.settings.group === mode ? "true" : "false",
    onclick: async () => {
      state.settings.group = mode;
      saveSettings();
      syncSettingsForm();
      await renderTimeline();
    },
  }, label);
}

async function ensureDay(date) {
  if (state.loadedDays.has(date)) return;
  try {
    const res = await fetch(`data/day/${date}.json?v=${state.cacheToken}`, { cache: "no-store" });
    state.loadedDays.set(date, await res.json());
  } catch {
    state.loadedDays.set(date, { date, cards: [], items: [] });
  }
}

function observeAutoLoadMore(node, callback, rootMargin = AUTO_ITEM_LOAD_ROOT_MARGIN, options = {}) {
  if (!node || typeof callback !== "function" || typeof IntersectionObserver === "undefined") return null;
  let busy = false;
  let lastScrollY = window.scrollY || 0;
  const matchesScrollIntent = () => {
    if (!options.scrollDirection) return true;
    const currentScrollY = window.scrollY || 0;
    const delta = currentScrollY - lastScrollY;
    lastScrollY = currentScrollY;
    if (Math.abs(delta) < 4) return false;
    return options.scrollDirection === "down" ? delta > 0 : delta < 0;
  };
  const observer = new IntersectionObserver((entries) => {
    if (busy || !entries.some((entry) => entry.isIntersecting)) return;
    if (options.notBefore && Date.now() < options.notBefore) return;
    if (!matchesScrollIntent()) return;
    busy = true;
    Promise.resolve(callback()).finally(() => {
      window.setTimeout(() => { busy = false; }, 180);
    });
  }, { root: null, rootMargin, threshold: 0.01 });
  observer.observe(node);
  return observer;
}

function setRefreshStatus(kind = "", text = "") {
  const status = $("#refresh-status");
  const btn = $("#refresh-btn");
  status.textContent = text || latestUpdateText();
  status.setAttribute("data-state", kind || "idle");
  btn.setAttribute("data-state", kind || "idle");
  btn.disabled = kind === "loading";
}

async function reloadIndex() {
  state.cacheToken = Date.now();
  const res = await fetch(`data/index.json?v=${state.cacheToken}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`刷新 index 失败：${res.status}`);
  const raw = await res.text();
  try {
    state.index = raw ? JSON.parse(raw) : { days: [] };
  } catch {
    await new Promise((resolve) => window.setTimeout(resolve, 180));
    const retry = await fetch(`data/index.json?v=${Date.now()}`, { cache: "no-store" });
    if (!retry.ok) throw new Error(`刷新 index 失败：${retry.status}`);
    const retryRaw = await retry.text();
    state.index = retryRaw ? JSON.parse(retryRaw) : { days: [] };
  }
  renderSidebar();
  syncIndexMeta();
}

async function loadScoringConfig() {
  if (IS_STATIC_SITE) return setScoringConfig(state.scoringConfig);
  const res = await fetch("/api/config/scoring", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取评分配置失败：${res.status}`);
  setScoringConfig(await res.json());
}

async function loadSourceConfig() {
  if (IS_STATIC_SITE) return setSourceConfig({ sources: state.index?.sources || [], categories: state.index?.categories || [] });
  const res = await fetch("/api/config/sources", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取信源配置失败：${res.status}`);
  setSourceConfig(await res.json());
}

async function loadTaggingConfig() {
  if (IS_STATIC_SITE) return setTaggingConfig(state.taggingConfig);
  const res = await fetch("/api/config/tagging", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取标签配置失败：${res.status}`);
  setTaggingConfig(await res.json());
}

async function loadDigestConfig() {
  if (IS_STATIC_SITE) return setDigestConfig(state.digestConfig);
  const res = await fetch("/api/config/daily-digest", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取日报配置失败：${res.status}`);
  setDigestConfig(await res.json());
}

async function loadScoringStatus() {
  if (IS_STATIC_SITE) return setScoringStatus({ ...STATIC_SITE_RUNTIME_STATUS, config: state.scoringConfig });
  const res = await fetch("/api/scoring/status", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取评分状态失败：${res.status}`);
  setScoringStatus(await res.json());
  const selected = getSelectedRuntimeJob();
  const currentJobId = state.scoringStatus.current_job?.job_id || null;
  if (selected?.job_id && selected.job_id !== currentJobId && !["queued", "cooldown"].includes(selected.status) && !state.runtimeJobDetails.has(selected.job_id)) {
    loadRuntimeJobDetail(selected.job_id)
      .then(() => renderScoringRuntime())
      .catch(() => {});
  }
}

async function loadTaggingJobDetail(jobId) {
  if (!jobId || state.taggingJobDetails.has(jobId)) return state.taggingJobDetails.get(jobId);
  const res = await fetch(`/api/tagging/jobs/${encodeURIComponent(jobId)}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`读取标签任务详情失败：${res.status}`);
  const detail = await res.json();
  state.taggingJobDetails.set(jobId, detail);
  return detail;
}

async function loadTaggingStatus() {
  if (IS_STATIC_SITE) return setTaggingStatus({ ...STATIC_SITE_RUNTIME_STATUS, config: state.taggingConfig });
  const res = await fetch("/api/tagging/status", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取标签状态失败：${res.status}`);
  setTaggingStatus(await res.json());
  const selected = getSelectedTaggingJob();
  const currentJobId = state.taggingStatus.current_job?.job_id || null;
  if (selected?.job_id && selected.job_id !== currentJobId && !["queued", "cooldown"].includes(selected.status) && !state.taggingJobDetails.has(selected.job_id)) {
    loadTaggingJobDetail(selected.job_id)
      .then(() => renderTaggingRuntime())
      .catch(() => {});
  }
}

async function loadDigestStatus() {
  if (IS_STATIC_SITE) return setDigestStatus({ ...STATIC_SITE_RUNTIME_STATUS, config: state.digestConfig });
  const res = await fetch("/api/digest/status", { cache: "no-store" });
  if (!res.ok) throw new Error(`读取日报状态失败：${res.status}`);
  setDigestStatus(await res.json());
}

async function loadSourceStatus(force = false) {
  if (IS_STATIC_SITE) {
    state.sourceStatus = { ...STATIC_SITE_SOURCE_STATUS, force: Boolean(force) };
    renderSourceAlert();
    return state.sourceStatus;
  }
  const suffix = force ? "?force=1" : "";
  const signal = typeof AbortSignal !== "undefined" && AbortSignal.timeout
    ? AbortSignal.timeout(2500)
    : undefined;
  const res = await fetch(`/api/source/status${suffix}`, { cache: "no-store", signal });
  if (!res.ok) throw new Error(`读取同步状态失败：${res.status}`);
  state.sourceStatus = await res.json();
  renderSourceAlert();
}

async function loadDigestIndex() {
  if (state.privateBundle) {
    const dates = getAvailableDigestDates();
    if (!state.selectedDigestDate || !dates.includes(state.selectedDigestDate)) {
      state.selectedDigestDate = dates[0] || null;
    }
    renderDigestDateSelect();
    return state.digestIndex;
  }
  const res = await fetch(`data/digest/index.json?v=${state.cacheToken}`, { cache: "no-store" });
  if (!res.ok) {
    state.digestIndex = { dates: [] };
    return state.digestIndex;
  }
  const raw = await res.text();
  try {
    state.digestIndex = raw ? JSON.parse(raw) : { dates: [] };
  } catch {
    await new Promise((resolve) => window.setTimeout(resolve, 180));
    const retry = await fetch(`data/digest/index.json?v=${Date.now()}`, { cache: "no-store" });
    const retryRaw = retry.ok ? await retry.text() : "";
    state.digestIndex = retryRaw ? JSON.parse(retryRaw) : { dates: [] };
  }
  const dates = getAvailableDigestDates();
  if (!state.selectedDigestDate || !dates.includes(state.selectedDigestDate)) {
    state.selectedDigestDate = dates[0] || null;
  }
  renderDigestDateSelect();
  return state.digestIndex;
}

function getAvailableDigestDates(tabKey = state.activeDigestTab) {
  if (tabKey === "kazike-daily") {
    return (state.index?.days || [])
      .filter((day) => Number(day?.categories?.["卡兹克日报"] || 0) > 0)
      .map((day) => day.date);
  }
  return state.digestIndex.dates || [];
}

function addIsoDays(dateStr, days) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dateStr || ""))) return dateStr || "";
  const [year, month, day] = dateStr.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day + Number(days || 0)));
  return date.toISOString().slice(0, 10);
}

function digestDateOffset(tabKey = state.activeDigestTab) {
  // 卡兹克日报本身已经按“发布日/日报日”落盘；本地 Digest 和 AI 精选
  // 目前文件键仍是内容窗口日期，所以界面上 +1 天显示为日报发布日。
  return tabKey === "kazike-daily" || tabKey === "gorden-daily" ? 0 : 1;
}

function digestIssueDate(sourceDate, tabKey = state.activeDigestTab) {
  return addIsoDays(sourceDate, digestDateOffset(tabKey));
}

function digestSourceDateFromIssueDate(issueDate, tabKey = state.activeDigestTab) {
  return addIsoDays(issueDate, -digestDateOffset(tabKey));
}

function resolveDigestSourceDate(issueDate, tabKey = state.activeDigestTab) {
  const dates = getAvailableDigestDates(tabKey);
  const candidate = digestSourceDateFromIssueDate(issueDate, tabKey);
  return dates.includes(candidate) ? candidate : null;
}

function formatDigestDateOption(sourceDate, tabKey = state.activeDigestTab) {
  const issueDate = digestIssueDate(sourceDate, tabKey);
  if (!issueDate || issueDate === sourceDate) return sourceDate;
  return `${issueDate}（内容 ${sourceDate.slice(5)}）`;
}

function formatDigestWeekday(sourceDate, tabKey = state.activeDigestTab) {
  const issueDate = digestIssueDate(sourceDate, tabKey);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(issueDate || ""))) return "";
  const date = new Date(`${issueDate}T00:00:00+08:00`);
  return ["周日", "周一", "周二", "周三", "周四", "周五", "周六"][date.getDay()] || "";
}

function formatDigestShortDate(sourceDate, tabKey = state.activeDigestTab) {
  const issueDate = digestIssueDate(sourceDate, tabKey);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(issueDate || ""))) return issueDate || sourceDate || "";
  return `${issueDate.slice(5, 7)}/${issueDate.slice(8, 10)}`;
}

function getDigestDateWindow(dates, selectedDate, radius = 3) {
  if (!dates.length) return [];
  const idx = Math.max(0, dates.indexOf(selectedDate));
  const selected = dates[idx] || selectedDate || dates[0];
  const slots = [];
  for (let slotIdx = idx - radius; slotIdx <= idx + radius; slotIdx += 1) {
    if (slotIdx >= 0 && slotIdx < dates.length) {
      slots.push({ date: dates[slotIdx], available: true });
    } else {
      // 固定 7 个位置。最新日报附近不够 7 天时，用灰色未来/历史占位，
      // 这样“轻阅读/刷新摘要”等按钮不会因为日期数量不同而跳位置。
      const offsetDays = idx - slotIdx;
      slots.push({ date: addIsoDays(selected, offsetDays), available: false });
    }
  }
  return slots;
}

function selectDigestDate(date) {
  const dates = getAvailableDigestDates();
  if (!date || !dates.includes(date)) return false;
  state.selectedDigestDate = date;
  renderDigestView();
  return true;
}

function shiftDigestDate(delta) {
  const dates = getAvailableDigestDates();
  if (!dates.length) return;
  const idx = dates.indexOf(state.selectedDigestDate);
  const baseIdx = idx >= 0 ? idx : 0;
  const next = dates[Math.max(0, Math.min(dates.length - 1, baseIdx + delta))];
  if (next && next !== state.selectedDigestDate) selectDigestDate(next);
}

function renderDigestDateSelect() {
  const select = $("#digest-date-select");
  const prevBtn = $("#digest-date-prev-btn");
  const nextBtn = $("#digest-date-next-btn");
  const dateInput = $("#digest-date-input");
  const rail = $("#digest-date-rail");
  if (!select) return;
  const dates = getAvailableDigestDates();
  if (!state.selectedDigestDate || !dates.includes(state.selectedDigestDate)) {
    state.selectedDigestDate = dates[0] || null;
  }
  const selectedIdx = dates.indexOf(state.selectedDigestDate);
  select.innerHTML = "";
  for (const date of dates) {
    select.appendChild(h("option", { value: date }, formatDigestDateOption(date)));
  }
  select.value = state.selectedDigestDate || "";
  if (prevBtn) {
    prevBtn.disabled = selectedIdx < 0 || selectedIdx >= dates.length - 1;
    prevBtn.title = "切到更早一天的日报";
  }
  if (nextBtn) {
    nextBtn.disabled = selectedIdx <= 0;
    nextBtn.title = "切到更新一天的日报";
  }
  if (dateInput) {
    dateInput.value = state.selectedDigestDate ? digestIssueDate(state.selectedDigestDate) : "";
    const issueDates = dates.map((date) => digestIssueDate(date)).filter(Boolean).sort();
    dateInput.min = issueDates[0] || "";
    dateInput.max = issueDates[issueDates.length - 1] || "";
  }
  if (rail) {
    rail.innerHTML = "";
    for (const slot of getDigestDateWindow(dates, state.selectedDigestDate)) {
      const date = slot.date;
      const isActive = date === state.selectedDigestDate;
      rail.appendChild(h("button", {
        type: "button",
        class: `digest-date-pill${isActive ? " active" : ""}${slot.available ? "" : " is-placeholder"}`,
        onclick: slot.available ? () => selectDigestDate(date) : null,
        "aria-current": isActive ? "date" : null,
        disabled: slot.available ? null : true,
        title: slot.available ? formatDigestDateOption(date) : "暂无日报，占位保持布局稳定",
      }, [
        h("strong", {}, formatDigestShortDate(date)),
        h("span", {}, formatDigestWeekday(date)),
      ]));
    }
  }
}

function renderDigestTabsBar() {
  const tabs = $("#digest-tabs");
  if (!tabs) return;
  const availableTabs = [
    { key: "local", label: "本地Digest" },
    { key: "ai-daily", label: "AI精选日报" },
    { key: "gorden-daily", label: "Gorden日报" },
    { key: "kazike-daily", label: "卡兹克日报" },
  ];
  if (state.activeDigestTab === "ai-pm") state.activeDigestTab = "ai-daily";
  if (!availableTabs.some((tab) => tab.key === state.activeDigestTab)) state.activeDigestTab = "local";
  tabs.innerHTML = "";
  for (const tab of availableTabs) {
    const hasData = tab.key === "kazike-daily"
      ? getAvailableDigestDates("kazike-daily").length > 0
      : (state.digestIndex.dates || []).length > 0;
    tabs.appendChild(h("button", {
      type: "button",
      class: `runtime-tab${state.activeDigestTab === tab.key ? " active" : ""}`,
      onclick: () => {
        const issueDate = digestIssueDate(state.selectedDigestDate, state.activeDigestTab);
        state.activeDigestTab = tab.key;
        state.selectedDigestDate = resolveDigestSourceDate(issueDate, tab.key) || getAvailableDigestDates(tab.key)[0] || null;
        renderDigestView();
      },
      disabled: hasData ? null : true,
    }, tab.label));
  }
}

function isDigestBriefMode() {
  return state.settings.digestReadMode === "brief";
}

function digestSupportsBriefMode(tabKey = state.activeDigestTab) {
  return !!tabKey;
}

function syncDigestReadModeButton() {
  const btn = $("#digest-read-mode-btn");
  if (!btn) return;
  const supported = digestSupportsBriefMode();
  btn.hidden = !supported;
  btn.disabled = !supported;
  if (!supported) {
    btn.classList.remove("active");
    btn.setAttribute("aria-pressed", "false");
    return;
  }
  const active = isDigestBriefMode();
  btn.classList.toggle("active", active);
  btn.setAttribute("aria-pressed", active ? "true" : "false");
  btn.textContent = active ? "轻阅读 Beta：开" : "轻阅读 Beta";
}

function toggleDigestReadMode() {
  if (!digestSupportsBriefMode()) return;
  state.settings.digestReadMode = isDigestBriefMode() ? "full" : "brief";
  saveSettings();
  syncDigestReadModeButton();
  renderDigestView();
}

function shortDigestText(value, max = 112) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  if (!text || text.length <= max) return text;
  return `${text.slice(0, max).replace(/[，。；：、,.!?！？;:：\s]+$/, "")}…`;
}

function renderDigestBriefBoard(title, subtitle, rows = [], options = {}) {
  const stats = Array.isArray(options.stats) ? options.stats.filter(Boolean) : [];
  return h("section", { class: `digest-brief-board${options.variant ? ` is-${options.variant}` : ""}` }, [
    h("div", { class: "digest-brief-board-head" }, [
      h("div", { class: "panel-kicker" }, options.eyebrow || "BETA · 轻阅读"),
      h("div", { class: "digest-brief-board-titleline" }, [
        h("h3", {}, title),
        stats.length ? h("div", { class: "digest-brief-stats" }, stats.map((stat) => h("span", {}, stat))) : null,
      ].filter(Boolean)),
      subtitle ? h("p", {}, subtitle) : null,
    ].filter(Boolean)),
    options.beforeGrid || null,
    rows.length ? h("div", { class: "digest-brief-grid" }, rows.slice(0, options.limit || 8).map((row, idx) => {
      const action = row.onClick
        ? h("button", { type: "button", class: "digest-brief-action", onclick: row.onClick }, row.actionLabel || "看详情")
        : (row.url ? h("a", { href: row.url, target: "_blank", rel: "noopener", class: "digest-brief-action" }, row.actionLabel || "打开原文 ↗") : null);
      return h("article", { class: `digest-brief-card${row.variant ? ` is-${row.variant}` : ""}` }, [
        h("div", { class: "digest-brief-card-badge" }, row.badge || String(idx + 1)),
        h("div", { class: "digest-brief-card-main" }, [
          row.kicker ? h("div", { class: "digest-brief-card-kicker" }, row.kicker) : null,
          h("strong", {}, row.title || "未命名"),
          row.text ? h("p", {}, shortDigestText(row.text, row.max || 112)) : null,
          row.meta ? h("div", { class: "digest-brief-meta" }, row.meta) : null,
          (row.chips || []).length ? h("div", { class: "digest-brief-chips" }, (row.chips || []).slice(0, 4).map((chip) => h("span", {}, shortDigestText(chip, 28)))) : null,
          action,
        ].filter(Boolean)),
      ]);
    })) : null,
  ].filter(Boolean));
}

async function loadDigest(date) {
  if (!date) return null;
  if (state.digestCache.has(date)) return state.digestCache.get(date);
  const url = IS_STATIC_SITE
    ? `data/digest/${encodeURIComponent(date)}.json?v=${state.cacheToken}`
    : `/api/digest/${encodeURIComponent(date)}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`读取 digest 失败：${res.status}`);
  const payload = await res.json();
  state.digestCache.set(date, payload);
  return payload;
}

function digestTabLabel(tabKey = state.activeDigestTab) {
  if (tabKey === "ai-daily") return "AI精选日报";
  if (tabKey === "gorden-daily") return "Gorden日报";
  if (tabKey === "kazike-daily") return "卡兹克日报";
  if (tabKey === "kazike") return "卡兹克源";
  return "本地 Digest";
}

function digestRefreshTimeoutMs(tabKey = state.activeDigestTab) {
  // 外部日报刷新会去源站慢速抓取，12 秒经常会前端超时但后端其实还在跑。
  // 给页面内骨架屏足够时间等待真实结果，避免“失败了但稍后才有数据”的错觉。
  if (tabKey === "local") return 45000;
  return 180000;
}

function setDigestViewRefreshing(isRefreshing) {
  const digestView = $("#digest-view");
  if (!digestView) return;
  digestView.toggleAttribute("data-refreshing", !!isRefreshing);
}

function renderDigestRefreshSkeleton(tabKey, sourceDate, message = "") {
  const content = $("#digest-content");
  if (!content) return;
  ++state.digestRenderSeq;
  const label = digestTabLabel(tabKey);
  const issueDate = digestIssueDate(sourceDate, tabKey) || sourceDate || "当前日期";
  setDigestViewRefreshing(true);
  const title = $("#digest-title");
  const subtitle = $("#digest-subtitle");
  if (title) title.textContent = `${label} · ${issueDate}`;
  if (subtitle) subtitle.textContent = message || "正在刷新，成功后会直接更新当前页面。";
  content.innerHTML = "";
  content.append(h("section", {
    class: "digest-card digest-refresh-skeleton",
    role: "status",
    "aria-live": "polite",
  }, [
    h("div", { class: "digest-refresh-skeleton-head" }, [
      h("div", { class: "digest-refresh-spinner", "aria-hidden": "true" }),
      h("div", {}, [
        h("div", { class: "panel-kicker" }, "Refreshing"),
        h("h3", {}, `正在刷新 ${label}`),
        h("p", {}, message || `正在拉取 ${issueDate} 的源头数据。拉取成功后，这里会自动换成最新日报内容。`),
      ]),
    ]),
    h("div", { class: "digest-refresh-skeleton-grid", "aria-hidden": "true" }, [
      h("div", { class: "digest-refresh-skeleton-card is-wide" }, [
        h("span", { class: "digest-skeleton-line is-short" }),
        h("span", { class: "digest-skeleton-line is-long" }),
        h("span", { class: "digest-skeleton-line" }),
        h("span", { class: "digest-skeleton-line is-mid" }),
      ]),
      h("div", { class: "digest-refresh-skeleton-card" }, [
        h("span", { class: "digest-skeleton-line is-short" }),
        h("span", { class: "digest-skeleton-line" }),
        h("span", { class: "digest-skeleton-line is-mid" }),
      ]),
      h("div", { class: "digest-refresh-skeleton-card" }, [
        h("span", { class: "digest-skeleton-line is-short" }),
        h("span", { class: "digest-skeleton-line is-long" }),
        h("span", { class: "digest-skeleton-line" }),
      ]),
    ]),
  ]));
}

function renderDigestRefreshError(tabKey, sourceDate, message) {
  const content = $("#digest-content");
  if (!content) return;
  ++state.digestRenderSeq;
  const label = digestTabLabel(tabKey);
  const issueDate = digestIssueDate(sourceDate, tabKey) || sourceDate || "当前日期";
  setDigestViewRefreshing(false);
  const title = $("#digest-title");
  const subtitle = $("#digest-subtitle");
  if (title) title.textContent = `${label} · ${issueDate}`;
  if (subtitle) subtitle.textContent = "刷新没有完成，原页面内容没有被硬替换。";
  content.innerHTML = "";
  content.append(h("section", { class: "digest-card digest-refresh-error", role: "alert" }, [
    h("div", { class: "digest-refresh-error-mark", "aria-hidden": "true" }, "!"),
    h("div", { class: "digest-refresh-error-copy" }, [
      h("div", { class: "panel-kicker" }, "Refresh failed"),
      h("h3", {}, `${label} 刷新失败`),
      h("p", {}, message || "源头暂时没有返回有效结果，可以稍后再试。"),
      h("div", { class: "digest-refresh-error-actions" }, [
        h("button", { type: "button", class: "digest-queue-action is-primary", onclick: () => refreshDigestCurrentDate() }, "重试刷新"),
        h("button", { type: "button", class: "digest-queue-action", onclick: () => renderDigestView() }, "回到刷新前页面"),
      ]),
    ]),
  ]));
}

function chooseDigestDateAfterRefresh(requestedDate, tabKey, returnedDates = []) {
  const available = new Set(getAvailableDigestDates(tabKey));
  const candidates = [requestedDate, ...returnedDates].filter(Boolean);
  for (const candidate of candidates) {
    if (!available.size || available.has(candidate)) return candidate;
  }
  return requestedDate || getAvailableDigestDates(tabKey)[0] || null;
}

async function refreshDigestCurrentDate() {
  if (IS_STATIC_SITE) {
    setRefreshStatus("idle", latestUpdateText());
    return;
  }
  if (state.digestRefreshing) return;
  const date = state.selectedDigestDate || getAvailableDigestDates()[0] || state.selectedDate || null;
  if (!date) {
    setRefreshStatus("error", "先选择一个日报日期");
    window.setTimeout(() => setRefreshStatus(), 3500);
    return;
  }
  state.digestRefreshing = true;
  const tab = state.activeDigestTab || "local";
  const label = digestTabLabel(tab);
  const issueDate = digestIssueDate(date, tab);
  const button = $("#digest-refresh-btn");
  if (button) button.disabled = true;
  renderDigestRefreshSkeleton(tab, date, `正在刷新 ${label} · ${issueDate || date}，成功后会直接更新当前页面。`);
  setRefreshStatus("loading", `正在刷新 ${label} · ${issueDate || date}…`);
  try {
    const signal = typeof AbortSignal !== "undefined" && AbortSignal.timeout
      ? AbortSignal.timeout(digestRefreshTimeoutMs(tab))
      : undefined;
    const res = await fetch("/api/digest/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal,
      body: JSON.stringify({ date, tab }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body.ok === false) throw new Error(body.error || `HTTP ${res.status}`);
    const affectedDates = new Set([date, ...(body.dates || [])]);
    for (const affectedDate of affectedDates) {
      state.digestCache.delete(affectedDate);
      state.loadedDays.delete(affectedDate);
    }
    state.cacheToken = Date.now();
    await reloadIndex().catch(() => {});
    await Promise.all([
      loadDigestIndex().catch(() => {}),
      loadSourceStatus(true).catch(() => {}),
      loadDigestStatus().catch(() => {}),
    ]);
    state.selectedDigestDate = chooseDigestDateAfterRefresh(date, tab, body.dates || []);
    renderSidebar();
    if (state.activeView === "feed") await renderTimeline();
    state.digestRefreshing = false;
    setDigestViewRefreshing(false);
    await renderDigestView();
    setRefreshStatus("success", body.message || `${label} 已刷新`);
  } catch (err) {
    const message = err?.name === "TimeoutError"
      ? `${label} 刷新等待时间较长，后台可能还在继续。稍后再点一次刷新或切回本日期查看。`
      : (err.message === "digest_refresh_in_progress"
        ? `${label} 正在刷新中，请稍等`
        : (err.message || `${label} 刷新失败`));
    setDigestViewRefreshing(false);
    renderDigestRefreshError(tab, date, message);
    setRefreshStatus("error", message);
  } finally {
    state.digestRefreshing = false;
    setDigestViewRefreshing(false);
    if (button) button.disabled = false;
    window.setTimeout(() => { if (!state.digestRefreshing) setRefreshStatus(); }, 5500);
  }
}

async function refreshLatestWindow() {
  if (IS_STATIC_SITE) {
    await checkForFreshData().catch(() => {});
    return;
  }
  if (state.refreshing) return;
  state.refreshing = true;
  setRefreshStatus("loading", "刷新中…");
  try {
    const signal = typeof AbortSignal !== "undefined" && AbortSignal.timeout
      ? AbortSignal.timeout(12000)
      : undefined;
    const res = await fetch("/api/refresh", { method: "POST", headers: { "Content-Type": "application/json" }, signal, body: "{}" });
    const body = await res.json().catch(() => ({}));
    if (!res.ok || body.ok === false) throw new Error(body.error || `HTTP ${res.status}`);
    for (const date of body.dates || []) state.loadedDays.delete(date);
    await reloadIndex();
    await loadUserStates().catch(() => {});
    await Promise.all([loadScoringStatus(), loadTaggingStatus(), loadDigestStatus(), loadSourceStatus(true).catch(() => {}), loadDigestIndex().catch(() => {})]);
    if (state.activeView === "feed") await renderTimeline();
    if (state.activeView === "digest") await renderDigestView();
    setRefreshStatus("success", (body.dates || []).length ? `已更新 ${body.dates.join(", ")}` : "过去 60 分钟暂无新增");
  } catch (err) {
    const message = err?.name === "TimeoutError"
      ? "刷新超时，请稍后再试"
      : (err.message === "refresh_in_progress" ? "已有刷新任务在跑，请稍等" : (err.message || "刷新失败"));
    setRefreshStatus("error", message);
  } finally {
    state.refreshing = false;
    window.setTimeout(() => { if (!state.refreshing) setRefreshStatus(); }, 4500);
  }
}

async function checkForFreshData() {
  if (state.refreshing) return false;
  const nextToken = Date.now();
  const res = await fetch(`data/index.json?v=${nextToken}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`检查最新数据失败：${res.status}`);
  const nextIndex = await res.json();
  if (currentIndexSignature(nextIndex) === currentIndexSignature()) return false;
  const previousScrollY = window.scrollY;
  state.cacheToken = nextToken;
  state.index = nextIndex;
  state.loadedDays.clear();
  renderSidebar();
  syncIndexMeta();
  await loadUserStates().catch(() => {});
  await loadDigestIndex().catch(() => {});
  await loadSourceStatus().catch(() => {});
  if (state.activeView === "feed") await renderTimeline();
  if (state.activeView === "digest") await renderDigestView();
  window.scrollTo({ top: previousScrollY, behavior: "auto" });
  setRefreshStatus("success", `已自动更新 · ${formatIndexTimestamp(nextIndex.generated_at) || "刚刚"}`);
  window.setTimeout(() => { if (!state.refreshing) setRefreshStatus(); }, 4500);
  return true;
}

async function renderDigestView() {
  const content = $("#digest-content");
  if (!content) return;
  const renderSeq = ++state.digestRenderSeq;
  const isStaleDigestRender = () => renderSeq !== state.digestRenderSeq;
  const digestView = content.closest("#digest-view");
  if (digestView) {
    digestView.setAttribute("data-read-mode", (isDigestBriefMode() && digestSupportsBriefMode()) ? "brief" : "full");
    if (!state.digestRefreshing) digestView.removeAttribute("data-refreshing");
  }
  syncDigestReadModeButton();
  renderDigestTabsBar();
  renderDigestDateSelect();
  const date = state.selectedDigestDate || getAvailableDigestDates()[0] || null;
  if (!date) {
    const emptyTitle = state.activeDigestTab === "kazike-daily"
      ? "卡兹克日报"
      : state.activeDigestTab === "gorden-daily"
        ? "Gorden日报"
        : "Daily Digest";
    const emptySubtitle = state.activeDigestTab === "kazike-daily"
      ? "还没有导入过卡兹克日报。"
      : state.activeDigestTab === "gorden-daily"
        ? "还没有导入过 Gorden AI 资讯日报。"
        : "还没有生成过日报总结。";
    $("#digest-title").textContent = emptyTitle;
    $("#digest-subtitle").textContent = emptySubtitle;
    content.innerHTML = `<div class="runtime-empty">${state.activeDigestTab === "kazike-daily" ? "暂无卡兹克日报数据。" : state.activeDigestTab === "gorden-daily" ? "暂无 Gorden 日报数据。" : "暂无 digest 数据。"}</div>`;
    return;
  }
  if (state.activeDigestTab === "kazike-daily") {
    try {
      await ensureDay(date);
      if (isStaleDigestRender()) return;
      const day = state.loadedDays.get(date) || { items: [] };
      const rows = (day.items || []).filter((item) => item?._source === "aihot-daily");
      $("#digest-title").textContent = `卡兹克日报 · ${digestIssueDate(date, "kazike-daily")}`;
      $("#digest-subtitle").textContent = rows.length ? `共 ${rows.length} 条，按原日报栏目整理成可扫读目录。` : "这一天还没有卡兹克日报内容。";
      content.innerHTML = "";
      if (!rows.length) {
        content.innerHTML = '<div class="runtime-empty">这一天没有卡兹克日报。可以点右上角“刷新摘要”再从卡兹克源头拉一次。</div>';
        return;
      }
      const jumpToFeedDate = async (jumpDate) => {
        state.activeView = "feed";
        state.selectedDate = jumpDate;
        syncPrimaryViews();
        await renderTimeline();
      };
      const focusKazikeNode = (id) => {
        const target = document.getElementById(id);
        if (!target) return;
        target.scrollIntoView({ behavior: "smooth", block: "start" });
        target.classList.add("is-focused");
        if (target._kazikeFocusTimer) window.clearTimeout(target._kazikeFocusTimer);
        target._kazikeFocusTimer = window.setTimeout(() => target.classList.remove("is-focused"), 2200);
      };
      const groups = new Map();
      for (const row of rows) {
        const key = row.origin_category || row.raw_cat || "其他";
        if (!groups.has(key)) groups.set(key, []);
        groups.get(key).push(row);
      }
      const orderedGroups = [...groups.entries()].sort((a, b) => {
        const order = ["技巧与观点", "新闻", "产品", "模型", "开发者", "快讯", "其他"];
        const ai = order.includes(a[0]) ? order.indexOf(a[0]) : 50;
        const bi = order.includes(b[0]) ? order.indexOf(b[0]) : 50;
        if (ai !== bi) return ai - bi;
        if (b[1].length !== a[1].length) return b[1].length - a[1].length;
        return a[0].localeCompare(b[0], "zh-Hans-CN");
      });
      const sourceCount = new Set(rows.map((row) => row.origin_source || row.author || row.source).filter(Boolean)).size;
      const flashCount = rows.filter((row) => row.origin_kind === "flash" || row.raw_kind === "flash").length;
      const stats = [
        ["条目", rows.length],
        ["栏目", orderedGroups.length],
        ["来源", sourceCount],
        ["快讯", flashCount],
      ];
      const renderKazikeTags = (row) => {
        const tags = [
          ...(row.entity_tags || []).slice(0, 3),
          ...(row.topic_tags || []).slice(0, 3),
        ].filter(Boolean);
        if (!tags.length && row.pm_label) tags.push(row.pm_label);
        return tags.length ? h("div", { class: "digest-kazike-tags" }, tags.slice(0, 6).map((value) => h("span", { class: "digest-kazike-tag" }, value))) : null;
      };
      const renderKazikeItem = (row, idx, sectionId) => {
        const isFlash = row.origin_kind === "flash" || row.raw_kind === "flash";
        const sourceLine = [
          row.origin_source || row.author || row.source,
          isFlash ? "快讯" : "深读",
          row.published_at ? formatRuntimeTime(row.published_at) : null,
        ].filter(Boolean).join(" · ");
        return h("article", { class: `digest-kazike-item${isFlash ? " is-flash" : ""}` }, [
          h("div", { class: "digest-kazike-item-rank" }, String(idx + 1).padStart(2, "0")),
          h("div", { class: "digest-kazike-item-main" }, [
            h("div", { class: "digest-kazike-title" }, row.url
              ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, `${cleanTitle(row.title || "未命名条目")} ↗`)
              : cleanTitle(row.title || "未命名条目")),
            sourceLine ? h("div", { class: "digest-kazike-meta" }, sourceLine) : null,
            row.summary ? h("p", { class: "digest-kazike-summary" }, row.summary) : null,
            renderKazikeTags(row),
            h("div", { class: "digest-kazike-actions" }, [
              row.url ? h("a", { href: row.url, target: "_blank", rel: "noopener", class: "digest-queue-action is-primary" }, "打开原文") : null,
              h("button", { type: "button", class: "digest-queue-action", onclick: () => focusKazikeNode("kazike-toc") }, "回到目录"),
              h("button", { type: "button", class: "digest-queue-action", onclick: () => jumpToFeedDate(date) }, "当天列表"),
            ].filter(Boolean)),
          ].filter(Boolean)),
        ]);
      };
      const renderKazikeNav = () => h("aside", { class: "digest-kazike-aside" }, [
        h("nav", { id: "kazike-toc", class: "digest-kazike-toc digest-focusable", "aria-label": "卡兹克日报快速导航" }, [
          h("div", { class: "digest-kazike-toc-title" }, [
            h("span", { class: "digest-pill-meta" }, "快速导航"),
            h("strong", {}, "按栏目跳转"),
          ]),
          ...orderedGroups.map(([section, sectionRows]) => {
            const sectionId = `kazike-section-${slugify(section)}`;
            return h("button", {
              type: "button",
              class: "digest-kazike-toc-chip",
              onclick: () => focusKazikeNode(sectionId),
            }, [h("span", { class: "digest-kazike-toc-label" }, section), h("span", { class: "digest-kazike-toc-count" }, String(sectionRows.length))]);
          }),
        ]),
      ]);
      const renderKazikeSections = () => h("div", { class: "digest-kazike-main" }, orderedGroups.map(([section, sectionRows]) => {
        const sectionId = `kazike-section-${slugify(section)}`;
        const sectionSources = new Set(sectionRows.map((row) => row.origin_source || row.author || row.source).filter(Boolean)).size;
        return h("section", { id: sectionId, class: "digest-card digest-kazike-section digest-focusable" }, [
          h("div", { class: "digest-kazike-section-head" }, [
            h("div", {}, [
              h("div", { class: "digest-pill-meta" }, "原日报栏目"),
              h("h3", { class: "digest-kazike-section-title" }, section),
            ]),
            h("div", { class: "digest-kazike-section-meta" }, [
              h("span", {}, `${sectionRows.length} 条`),
              h("span", {}, `${sectionSources} 个来源`),
            ]),
          ]),
          h("div", { class: `digest-kazike-list${section === "快讯" ? " is-flash-list" : ""}` }, sectionRows.map((row, idx) => renderKazikeItem(row, idx, sectionId))),
        ]);
      }));
      const renderKazikeBriefBoard = () => h("section", { class: "digest-brief-board is-kazike digest-kazike-brief-board" }, [
        h("div", { class: "digest-brief-board-head" }, [
          h("div", { class: "panel-kicker" }, "KAZIKE · 栏目标题速览"),
          h("div", { class: "digest-brief-board-titleline" }, [
            h("h3", {}, "先扫每个栏目标题，再决定往哪跳"),
            h("div", { class: "digest-brief-stats" }, [
              h("span", {}, `${rows.length} 条`),
              h("span", {}, `${orderedGroups.length} 个栏目`),
              h("span", {}, `${sourceCount} 个来源`),
              flashCount ? h("span", {}, `${flashCount} 条快讯`) : null,
            ].filter(Boolean)),
          ]),
          h("p", {}, "卡兹克日报更适合当“栏目目录”读：每栏先露出前几条标题，标题打动你再跳到下面完整卡片。"),
        ]),
        h("div", { class: "digest-kazike-brief-grid" }, orderedGroups.map(([section, sectionRows]) => {
          const sectionId = `kazike-section-${slugify(section)}`;
          const sectionSources = new Set(sectionRows.map((row) => row.origin_source || row.author || row.source).filter(Boolean)).size;
          return h("article", { class: "digest-kazike-brief-card" }, [
            h("div", { class: "digest-kazike-brief-head" }, [
              h("div", {}, [
                h("div", { class: "digest-kazike-brief-kicker" }, `${sectionRows.length} 条 · ${sectionSources} 个来源`),
                h("strong", {}, section),
              ]),
              h("button", { type: "button", class: "digest-brief-action", onclick: () => focusKazikeNode(sectionId) }, "跳到栏目"),
            ]),
            h("ol", { class: "digest-kazike-title-list" }, sectionRows.slice(0, 5).map((row) => h("li", {}, [
              row.url
                ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, cleanTitle(row.title || "未命名条目"))
                : h("span", {}, cleanTitle(row.title || "未命名条目")),
              h("em", {}, [row.origin_source || row.author || row.source, row.origin_kind === "flash" || row.raw_kind === "flash" ? "快讯" : ""].filter(Boolean).join(" · ")),
            ]))),
            sectionRows.length > 5 ? h("div", { class: "digest-kazike-brief-more" }, `还有 ${sectionRows.length - 5} 条，点“跳到栏目”继续看`) : null,
          ].filter(Boolean));
        })),
      ]);
      content.append(
        h("section", { class: "digest-card digest-kazike-shell" }, [
          h("div", { class: "digest-kazike-hero" }, [
            h("div", {}, [
              h("div", { class: "panel-kicker" }, "KAZIKE DAILY"),
              h("h3", { class: "digest-kazike-heading" }, "先看栏目，再决定要不要展开到原文"),
              h("p", { class: "digest-section-note" }, "这里保留源头日报的栏目结构，但把它改成目录 + 卡片：先扫标题和摘要，感兴趣再打开原文或跳回当天信息流。"),
            ]),
            h("div", { class: "digest-kazike-stat-grid" }, stats.map(([label, value]) => h("div", { class: "digest-kazike-stat" }, [
              h("strong", {}, String(value || 0)),
              h("span", {}, label),
            ]))),
          ]),
          isDigestBriefMode() ? renderKazikeBriefBoard() : null,
          h("div", { class: "digest-kazike-layout" }, [
            renderKazikeNav(),
            renderKazikeSections(),
          ]),
        ]),
      );
      return;
    } catch (err) {
      content.innerHTML = `<div class="runtime-empty">${err.message || "读取卡兹克日报失败"}</div>`;
      return;
    }
  }
  try {
    const digest = await loadDigest(date);
    if (isStaleDigestRender()) return;
    if (!digest) {
      content.innerHTML = '<div class="runtime-empty">暂无 digest 数据。</div>';
      return;
    }
    const issueDate = digestIssueDate(digest.date, state.activeDigestTab);
    $("#digest-title").textContent = `${digestTabLabel(state.activeDigestTab)} · ${issueDate}`;
    $("#digest-subtitle").textContent = `内容窗口 ${digest.date} · 标签覆盖 ${digest.coverage?.tagging_done || 0}/${digest.coverage?.items_total || 0} · 评分覆盖 ${digest.coverage?.scoring_done || 0}/${digest.coverage?.representative_total || 0} · ${digest.coverage?.tagging_note || ""}`;
    content.innerHTML = "";
    const renderBulletList = (rows = []) => h("ul", { class: "digest-news-list" }, rows.map((row) => h("li", {}, row)));
    const jumpToFeedDate = (date) => {
      state.activeView = "feed";
      state.selectedDate = date;
      toggleDigestView("feed");
      renderTimeline();
    };
    const renderExamples = (rows = []) => h("div", { class: "digest-example-list" }, rows.map((row) => h("article", { class: "digest-example-item" }, [
      h("div", { class: "digest-example-title" }, row.url
        ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, `${row.title || row.item_id || "原始样本"} ↗`)
        : (row.title || row.item_id || "原始样本")),
      h("div", { class: "digest-example-meta" }, [row.source, row.segment, row.pm_label].filter(Boolean).join(" · ")),
      h("div", { class: "digest-example-actions" }, [
        row.url ? h("a", { href: row.url, target: "_blank", rel: "noopener", class: "digest-inline-link" }, "原文") : null,
        h("button", { type: "button", class: "digest-inline-link digest-inline-button", onclick: () => jumpToFeedDate(digest.date) }, "当天列表"),
      ].filter(Boolean)),
    ])));
    const renderDigestRelated = (label, values = [], kind = "entity") => {
      if (!values.length) return null;
      return h("div", { class: "digest-related-row" }, [
        h("span", { class: "digest-related-label" }, label),
        h("div", { class: "digest-related-pills" }, values.map((value) => h("span", {
          class: `tag-pill ${kind === "topic" ? "tag-pill-topic" : "tag-pill-entity"}`,
        }, value))),
      ]);
    };
    const renderModelTop5 = (rows, title) => h("section", { id: "digest-local-top5", class: "digest-card digest-top-section digest-focusable" }, [
      h("div", { class: "digest-card-head" }, [h("strong", {}, title)]),
      h("div", { class: "digest-top-list" }, rows.map((row, idx) => h("article", { class: "digest-top-row" }, [
        h("div", { class: "digest-top-rank" }, [`TOP ${idx + 1}`]),
        h("div", { class: "digest-top-main" }, [
          h("div", { class: "digest-pill-title" }, row.title || "未命名热点"),
          (row.mention_count || row.story_count) ? h("div", { class: "digest-top-stats" }, [
            row.mention_count ? `提到 ${row.mention_count} 次` : null,
            row.story_count ? `聚合 ${row.story_count} 条故事` : null,
          ].filter(Boolean).join(" · ")) : null,
          renderDigestRelated("相关实体", row.related_entities || [], "entity"),
          renderDigestRelated("相关话题", row.related_topics || [], "topic"),
          row.what_happened ? h("p", { class: "digest-pill-copy" }, row.what_happened) : null,
          row.why_hot ? h("div", { class: "digest-pill-meta" }, `昨日热点：${row.why_hot}`) : null,
        ]),
        (row.examples || []).length ? h("div", { class: "digest-pill-examples" }, [
          h("div", { class: "digest-example-head" }, "原始信息"),
          renderExamples((row.examples || []).slice(0, 4)),
        ]) : h("div", { class: "digest-pill-examples digest-pill-examples-empty" }, "暂无原始样本"),
      ]))),
    ]);
    const renderPlatformHotspots = (sections = []) => {
      if (!sections.length) return null;
      return h("section", { id: "digest-local-platforms", class: "digest-card digest-focusable" }, [
        h("div", { class: "digest-card-head" }, [h("strong", {}, "分平台热点")]),
        h("div", { class: "digest-platform-list" }, sections.map((section) => h("section", { class: "digest-platform-card" }, [
          h("div", { class: "digest-platform-head" }, [
            h("div", { class: "digest-platform-title" }, section.source || "未知来源"),
            h("div", { class: "digest-platform-meta" }, `高价值 ${section.high_value_story_count || 0} · 总条数 ${section.item_count || 0}`),
          ]),
          (section.rows || []).length
            ? h("div", { class: "digest-platform-rows" }, (section.rows || []).map((row, idx) => h("article", { class: "digest-platform-row" }, [
                h("div", { class: "digest-platform-rank" }, `#${idx + 1}`),
                h("div", { class: "digest-platform-main" }, [
                  h("div", { class: "digest-pill-title" }, row.title || "未命名热点"),
                  (row.mention_count || row.story_count) ? h("div", { class: "digest-top-stats" }, [
                    row.mention_count ? `提到 ${row.mention_count} 次` : null,
                    row.story_count ? `聚合 ${row.story_count} 条故事` : null,
                  ].filter(Boolean).join(" · ")) : null,
                  renderDigestRelated("相关实体", row.related_entities || [], "entity"),
                  renderDigestRelated("相关话题", row.related_topics || [], "topic"),
                ]),
              ])))
            : h("div", { class: "digest-platform-empty" }, section.empty_reason || "暂无热点"),
        ]))),
      ]);
    };
    const renderDetailBlock = (title, body) => {
      if (!body) return null;
      return h("div", {}, [h("strong", {}, title), typeof body === "string" ? h("p", {}, body) : body]);
    };
    const renderDetailList = (title, values = []) => {
      if (!values.length) return null;
      return renderDetailBlock(title, renderBulletList(values));
    };
    const renderScoreChips = (pairs = []) => {
      const rows = pairs.filter((row) => row && row.value != null && row.value !== "" && !Number.isNaN(Number(row.value)));
      if (!rows.length) return null;
      return h("div", { class: "digest-score-chips" }, rows.map((row) => h("span", {
        class: `digest-score-chip${row.tone ? ` is-${row.tone}` : ""}`,
      }, `${row.label} ${Math.round(Number(row.value) || 0)}`)));
    };
    const digestTextKey = (value) => String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[\s，。！？、,.!?;；:：·|｜/\\（）()[\]【】「」『』"'“”‘’《》<>_-]+/g, "");
    const uniqueDigestText = (value, existing = []) => {
      const text = String(value || "").trim();
      if (!text) return "";
      const key = digestTextKey(text);
      if (!key) return "";
      for (const item of existing) {
        const other = digestTextKey(item);
        if (!other) continue;
        if (key === other) return "";
        if (other.length >= 12 && key.endsWith(other) && key.length - other.length <= 12) return "";
      }
      return text;
    };
    const digestArticleKey = (row = {}) => {
      const url = String(row.url || "").trim();
      if (url) {
        return `u:${url
          .replace(/#.*$/, "")
          .replace(/[?&](utm_[^=]+|from|scene|clicktime)=[^&]+/g, "")
          .replace(/[?&]$/, "")
          .replace(/\/+$/, "")
          .toLowerCase()}`;
      }
      return `t:${digestTextKey(row.title || row.item_id || "")}`;
    };
    const digestIdHash = (value) => {
      let hash = 2166136261;
      const text = String(value || "");
      for (let i = 0; i < text.length; i += 1) {
        hash ^= text.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
      }
      return (hash >>> 0).toString(36);
    };
    const digestAnalysisId = (row = {}) => `digest-pm-analysis-${digestIdHash(digestArticleKey(row))}`;
    const digestPmQueueId = (row = {}) => `digest-pm-source-${digestIdHash(digestArticleKey(row))}`;
    const digestRemainingTocId = (row = {}) => `digest-remaining-source-${digestIdHash(digestArticleKey(row))}`;
    const digestRemainingDetailId = (row = {}) => `digest-remaining-detail-${digestIdHash(digestArticleKey(row))}`;
    const buildDigestAnalysisMap = (rows = []) => {
      const map = new Map();
      rows.forEach((row) => {
        const keys = [
          digestArticleKey(row),
          row.title ? `t:${digestTextKey(row.title)}` : "",
        ].filter(Boolean);
        keys.forEach((key) => {
          if (key && !map.has(key)) map.set(key, row);
        });
      });
      return map;
    };
    const findDigestAnalysis = (row = {}, analysisMap = new Map()) => {
      if (!row || !analysisMap.size) return null;
      return analysisMap.get(digestArticleKey(row))
        || analysisMap.get(`t:${digestTextKey(row.title || "")}`)
        || null;
    };
    const focusDigestNode = (targetId, options = {}) => {
      const target = targetId ? document.getElementById(targetId) : null;
      if (!target) return;
      target.closest("details")?.setAttribute("open", "");
      if (options.openDetails !== false) {
        const details = target.querySelector("details.digest-analysis-details");
        if (details) details.open = true;
      }
      if (options.returnTarget) target.dataset.returnTarget = options.returnTarget;
      target.scrollIntoView({ behavior: "smooth", block: "start" });
      target.classList.remove("is-focused");
      // Force reflow so repeated clicks replay the highlight.
      void target.offsetWidth;
      target.classList.add("is-focused");
      window.clearTimeout(target._digestFocusTimer);
      target._digestFocusTimer = window.setTimeout(() => target.classList.remove("is-focused"), 2600);
    };
    const focusDigestAnalysis = (analysisId, returnTarget = "") => {
      focusDigestNode(analysisId, { returnTarget });
    };
    const renderParagraphBlock = (title, text) => {
      const rows = String(text || "").replace(/\r/g, "").split(/\n{2,}/).map((row) => row.trim()).filter(Boolean);
      if (!rows.length) return null;
      return h("div", { class: "digest-analysis-longform" }, [
        title ? h("strong", {}, title) : null,
        ...rows.map((row) => h("p", {}, row)),
      ].filter(Boolean));
    };
    const chooseArticleSummary = (row, pm, variant = "") => {
      const summary = uniqueDigestText(row.summary || row.description || "");
      if (!summary) return "";
      if (variant === "top-pick") return "";
      if (pm && summary.length <= 24) return "";
      return summary;
    };
    const renderCompactPointGroup = (title, values = [], maxRows = 2) => {
      const rows = (values || []).filter(Boolean).slice(0, maxRows);
      if (!rows.length) return null;
      return h("div", { class: "digest-ai-point-group" }, [
        h("strong", {}, title),
        h("ul", { class: "digest-ai-point-list" }, rows.map((row) => h("li", {}, row))),
      ]);
    };
    const renderAiArticleCard = (row, label = "", options = {}) => {
      const compact = options.compact !== false;
      const variant = options.variant || "";
      const cardId = options.id || "";
      const returnTarget = options.returnTarget || "";
      const returnLabel = options.returnLabel || "回到目录";
      const pm = row.pm_analysis || null;
      const meta = [
        row.source_name,
        row.category,
        row.pub_date ? row.pub_date.slice(0, 10) : null,
      ].filter(Boolean).join(" · ");
      const scoreChips = renderScoreChips([
        { label: "编辑优先级", value: row.editorial_score, tone: "decision" },
        { label: "最终分", value: pm?.post_analysis_score, tone: "signal" },
        { label: "原始分", value: row.score, tone: "neutral" },
      ]);
      const summaryText = chooseArticleSummary(row, pm, variant);
      const judgmentText = uniqueDigestText(pm?.one_line_judgment || "", [summaryText]);
      const reasonText = uniqueDigestText(pm?.post_analysis_reason || row.editorial_reason || row.reason || "", [
        summaryText,
        judgmentText,
        row.description,
        row.summary,
      ]);
      const detailRows = [
        (row.keywords || []).length ? h("div", { class: "digest-keyword-row" }, (row.keywords || []).slice(0, 10).map((value) => h("span", { class: "digest-keyword-chip" }, value))) : null,
        renderDetailList("最值钱的点", pm?.key_points || []),
        renderDetailList("容易忽略", pm?.missed_points || []),
        renderDetailList("影响", pm?.flattened_implications || []),
        renderDetailList("风险", pm?.risks || []),
        renderDetailBlock("适合谁读", pm?.recommendation?.who_should_read || ""),
        renderDetailBlock("谁可以跳过", pm?.recommendation?.who_can_skip || ""),
        renderDetailBlock("追问", pm?.recommendation?.follow_up_question || ""),
      ].filter(Boolean);
      return h("article", { id: cardId || null, class: `digest-insight-card digest-article-card digest-focusable${compact ? " is-compact" : ""}${variant ? ` is-${variant}` : ""}` }, [
        h("div", { class: "digest-article-head" }, [
          h("div", { class: "digest-article-head-main" }, [
            label ? h("span", { class: "digest-rank-chip" }, label) : null,
            h("div", { class: "digest-insight-title digest-article-title" }, row.url
              ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, `${row.title || "未命名文章"} ↗`)
              : (row.title || "未命名文章")),
          ].filter(Boolean)),
          scoreChips,
        ]),
        meta ? h("div", { class: "digest-example-meta digest-article-meta" }, meta) : null,
        returnTarget ? h("div", { class: "digest-detail-return-row" }, [
          h("button", {
            type: "button",
            class: "digest-queue-action",
            onclick: () => focusDigestNode(returnTarget, { openDetails: false }),
          }, returnLabel),
        ]) : null,
        (row.editorial_signals || []).length ? h("div", { class: "digest-article-signal-row" }, (row.editorial_signals || []).slice(0, 4).map((value) => h("span", { class: "digest-article-signal-chip" }, value))) : null,
        pm?.recommendation?.verdict ? h("div", { class: "digest-pill-meta" }, pm.recommendation.verdict) : (pm?.conclusion ? h("div", { class: "digest-pill-meta" }, pm.conclusion) : null),
        judgmentText ? h("p", { class: "digest-mainline digest-article-judgment" }, judgmentText) : null,
        summaryText ? h("p", { class: "digest-article-summary" }, summaryText) : null,
        reasonText ? h("p", { class: "digest-article-reason" }, reasonText) : null,
        detailRows.length
          ? (compact
            ? h("details", { class: "digest-analysis-details digest-article-details" }, [
                h("summary", { class: "digest-analysis-toggle" }, "展开详细点评"),
                h("div", { class: "digest-analysis-sections digest-article-detail-grid" }, detailRows),
              ])
            : h("div", { class: "digest-analysis-sections digest-article-detail-grid" }, detailRows))
          : null,
      ].filter(Boolean));
    };
    const renderAiDailyTopPickCard = (row, idx) => {
      const pm = row.pm_analysis || {};
      const sourceLine = [row.source_name, row.category].filter(Boolean).join(" · ");
      const judgmentText = uniqueDigestText(pm?.one_line_judgment || row.editorial_reason || "");
      const reasonText = uniqueDigestText(pm?.post_analysis_reason || row.editorial_reason || row.reason || "", [judgmentText]);
      const implicationRows = pm?.flattened_implications || [];
      const riskRows = pm?.risks || [];
      const recommendation = pm?.recommendation || {};
      const renderTagChips = [
        sourceLine ? h("span", { class: "digest-ai-chip" }, sourceLine) : null,
        ...(row.editorial_signals || []).map((signal) => h("span", { class: "digest-ai-chip digest-ai-chip-strong" }, signal)),
        pm?.conclusion ? h("span", { class: "digest-ai-chip" }, pm.conclusion) : null,
        recommendation?.verdict ? h("span", { class: "digest-ai-chip" }, recommendation.verdict) : null,
      ].filter(Boolean);
      return h("article", { class: "digest-ai-top-pick-card" }, [
        h("div", { class: "digest-ai-top-pick-head" }, [
          h("div", { class: "digest-ai-top-pick-main" }, [
            h("div", { class: "digest-ai-top-pick-rank" }, `TOP ${idx + 1}`),
            h("div", { class: "digest-ai-top-pick-title" }, row.url
              ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, `${row.title || "未命名文章"} ↗`)
              : (row.title || "未命名文章")),
          ]),
          h("div", { class: "digest-ai-top-pick-scores" }, [
            row.editorial_score != null ? h("span", { class: "digest-ai-top-pick-score" }, `编辑优先级 ${Math.round(Number(row.editorial_score) || 0)}`) : null,
            pm?.post_analysis_score != null ? h("span", { class: "digest-ai-top-pick-score digest-ai-top-pick-score-secondary" }, `PM 最终分 ${Math.round(Number(pm.post_analysis_score) || 0)}`) : null,
          ].filter(Boolean)),
        ]),
        renderTagChips.length ? h("div", { class: "digest-ai-chip-row" }, renderTagChips) : null,
        judgmentText ? h("p", { class: "digest-ai-top-pick-judgment" }, judgmentText) : null,
        reasonText ? h("p", { class: "digest-ai-top-pick-reason" }, reasonText) : null,
        h("div", { class: "digest-ai-top-pick-grid digest-ai-top-pick-grid-compact" }, [
          renderCompactPointGroup("最值钱的点", pm?.key_points || [], 2),
          renderCompactPointGroup("容易忽略", pm?.missed_points || [], 1),
        ].filter(Boolean)),
        h("details", { class: "digest-analysis-details digest-ai-top-pick-details" }, [
          h("summary", { class: "digest-analysis-toggle" }, "点击展开完整解读"),
          h("div", { class: "digest-ai-top-pick-actions" }, [
            row.url ? h("a", { href: row.url, target: "_blank", rel: "noopener", class: "digest-inline-link" }, "打开原文") : null,
          ].filter(Boolean)),
          h("div", { class: "digest-ai-top-pick-grid" }, [
            renderDetailList("影响", implicationRows),
            renderDetailList("风险", riskRows),
            renderDetailBlock("适合谁读", recommendation?.who_should_read || ""),
            renderDetailBlock("谁可以跳过", recommendation?.who_can_skip || ""),
            renderDetailBlock("追问", recommendation?.follow_up_question || ""),
          ].filter(Boolean)),
        ]),
      ]);
    };
    const renderAiPmQueue = (title, rows = [], analysisMap = new Map()) => {
      if (!rows.length) return null;
      const isWorth = /值得/.test(title);
      return h("section", { class: `digest-ai-queue ${isWorth ? "is-worth" : "is-optional"}` }, [
        h("div", { class: "digest-card-head digest-ai-queue-head" }, [
          h("strong", {}, title),
          h("span", { class: "digest-top-stats" }, `${rows.length} 篇`),
        ]),
        h("div", { class: "digest-pm-queue-list" }, rows.map((row, idx) => {
          const analysis = findDigestAnalysis(row, analysisMap);
          const analysisId = analysis ? digestAnalysisId(analysis) : "";
          const reasonText = uniqueDigestText(
            analysis?.one_line_judgment || row.reason || analysis?.post_analysis_reason || "",
            [row.excerpt, row.summary, row.title],
          );
          const jump = (event) => {
            event?.preventDefault?.();
            if (analysisId) focusDigestAnalysis(analysisId, digestPmQueueId(row));
            else if (row.url) window.open(row.url, "_blank", "noopener");
          };
          const onKeydown = (event) => {
            if (event.key !== "Enter" && event.key !== " ") return;
            event.preventDefault();
            jump(event);
          };
          return h("article", {
            id: digestPmQueueId(row),
            class: `digest-pm-queue-item digest-focusable${analysisId ? " is-jumpable" : ""}`,
            role: analysisId ? "button" : null,
            tabindex: analysisId ? "0" : null,
            onclick: jump,
            onkeydown: onKeydown,
          }, [
            h("div", { class: "digest-pm-queue-main" }, [
              h("div", { class: "digest-pm-queue-eyebrow" }, [
                h("span", { class: "digest-pill-meta" }, `TOP ${idx + 1}`),
                row.conclusion ? h("span", { class: "digest-pill-meta" }, row.conclusion) : null,
              ].filter(Boolean)),
              h("div", { class: "digest-pill-title digest-pm-queue-title" }, row.title || "未命名文章"),
              h("div", { class: "digest-top-stats" }, [row.source_name, row.post_analysis_score ? `PM ${Math.round(Number(row.post_analysis_score) || 0)}` : null].filter(Boolean).join(" · ")),
              reasonText ? h("p", { class: "digest-pm-queue-reason" }, reasonText) : null,
              h("div", { class: "digest-pm-queue-actions" }, [
                analysisId ? h("button", {
                  type: "button",
                  class: "digest-queue-action is-primary",
                  onclick: (event) => {
                    event.stopPropagation();
                    focusDigestAnalysis(analysisId, digestPmQueueId(row));
                  },
                }, "看解读") : null,
                row.url ? h("a", {
                  href: row.url,
                  target: "_blank",
                  rel: "noopener",
                  class: "digest-queue-action",
                  onclick: (event) => event.stopPropagation(),
                }, "原文 ↗") : null,
              ].filter(Boolean)),
            ]),
            row.post_analysis_score ? h("div", { class: "digest-pm-queue-rank" }, [Math.round(Number(row.post_analysis_score) || 0)]) : null,
          ].filter(Boolean));
        })),
      ]);
    };
    const renderAiPmRows = (rows = [], returnMap = new Map()) => {
      if (!rows.length) return null;
      return h("div", { class: "digest-analysis-grid" }, rows.map((row, idx) => {
        const returnTarget = returnMap.get(digestArticleKey(row)) || "";
        return h("article", { id: digestAnalysisId(row), class: "digest-analysis-card digest-focusable" }, [
          h("div", { class: "digest-analysis-head" }, [
            h("div", {}, [
            h("div", { class: "digest-pill-meta" }, `重点解读 ${idx + 1}`),
            h("div", { class: "digest-insight-title" }, row.url ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, `${row.title || "未命名文章"} ↗`) : (row.title || "未命名文章")),
            h("div", { class: "digest-top-stats" }, [row.source_name, row.post_analysis_score ? `最终分 ${Math.round(Number(row.post_analysis_score) || 0)}` : null, row.grade || row.conclusion || null].filter(Boolean).join(" · ")),
          ]),
          renderScoreChips([
            { label: "原始分", value: row.article_score, tone: "neutral" },
            { label: "最终分", value: row.post_analysis_score, tone: "decision" },
          ]),
        ]),
        returnTarget ? h("div", { class: "digest-detail-return-row" }, [
          h("button", {
            type: "button",
            class: "digest-queue-action",
            onclick: () => focusDigestNode(returnTarget, { openDetails: false }),
          }, "回到PM目录"),
        ]) : null,
        row.one_line_judgment ? h("p", { class: "digest-mainline" }, row.one_line_judgment) : null,
        row.post_analysis_reason ? h("p", { class: "digest-analysis-summary" }, row.post_analysis_reason) : null,
        h("div", { class: "digest-analysis-sections" }, [
          renderDetailList("最值钱的点", row.key_points || []),
          renderDetailList("容易忽略", row.missed_points || []),
          renderDetailList("核心原因", row.core_reasons || []),
          renderDetailList("影响", row.flattened_implications || []),
          renderDetailList("风险", row.risks || []),
        ].filter(Boolean)),
        h("div", { class: "digest-analysis-audience" }, [
          renderDetailBlock("适合谁读", row.recommendation?.who_should_read || ""),
          renderDetailBlock("谁可以跳过", row.recommendation?.who_can_skip || ""),
          renderDetailBlock("追问", row.recommendation?.follow_up_question || ""),
        ].filter(Boolean)),
        row.analysis_body ? h("details", { class: "digest-analysis-details" }, [
          h("summary", { class: "digest-analysis-toggle" }, "查看完整解读"),
          renderParagraphBlock("", row.analysis_body || ""),
        ]) : null,
      ].filter(Boolean));
      }));
    };
    const isAiDailyPayloadEmpty = (payload) => {
      if (!payload) return true;
      const count = payload.selected_count || payload.export_meta?.article_count || (payload.articles || []).length || 0;
      return count === 0 && !(payload.top_picks || []).length && !(payload.remaining_articles || []).length;
    };
    const renderAiDailyReport = (payload, notice = "") => {
      if (!payload) return h("section", { class: "digest-card" }, [
        h("div", { class: "digest-card-head" }, [h("strong", {}, "AI精选日报")]),
        h("div", { class: "runtime-empty" }, "当天还没有 AI 精选日报。"),
      ]);
      const articleCount = payload.selected_count || payload.export_meta?.article_count || (payload.articles || []).length || 0;
      if (articleCount === 0 && !(payload.top_picks || []).length && !(payload.remaining_articles || []).length) {
        return h("section", { class: "digest-card digest-ai-daily-shell" }, [
          h("div", { class: "digest-card-head" }, [h("strong", {}, "AI精选日报")]),
          h("div", { class: "digest-top-stats" }, ["精选 0 篇", payload.export_meta?.exported_at ? `源头导出 ${formatRuntimeTime(payload.export_meta.exported_at)}` : null].filter(Boolean).join(" · ")),
          h("div", { class: "runtime-empty" }, "源头这一天已经有日报导出，但返回 0 篇文章。可以稍后再点“刷新摘要”拉新版本。"),
        ]);
      }
      const pmDigest = payload.pm_digest || {};
      const pmAnalyses = pmDigest.analyses || [];
      const pmAnalysisMap = buildDigestAnalysisMap(pmAnalyses);
      const pmReturnMap = new Map();
      [...(pmDigest.worth_reading || []), ...(pmDigest.optional_reading || [])].forEach((row) => {
        pmReturnMap.set(digestArticleKey(row), digestPmQueueId(row));
      });
      const renderRemainingDirectory = (rows = []) => {
        if (!rows.length) return null;
        return h("div", { class: "digest-remaining-directory" }, rows.map((row, idx) => {
          const detailId = digestRemainingDetailId(row);
          const tocId = digestRemainingTocId(row);
          const pm = row.pm_analysis || {};
          const summaryText = uniqueDigestText(pm.one_line_judgment || row.editorial_reason || row.summary || row.description || "", [row.title]);
          const score = pm.post_analysis_score ?? row.editorial_score ?? row.score;
          const jump = (event) => {
            event?.preventDefault?.();
            focusDigestNode(detailId, { returnTarget: tocId });
          };
          const onKeydown = (event) => {
            if (event.key !== "Enter" && event.key !== " ") return;
            event.preventDefault();
            jump(event);
          };
          const sourceMeta = [row.source_name, row.category, row.pub_date ? row.pub_date.slice(0, 10) : null].filter(Boolean).join(" · ");
          return h("article", {
            id: tocId,
            class: "digest-remaining-toc-item digest-focusable",
            role: "button",
            tabindex: "0",
            onclick: jump,
            onkeydown: onKeydown,
          }, [
            h("div", { class: "digest-remaining-toc-main" }, [
              h("div", { class: "digest-remaining-toc-head" }, [
                h("div", { class: "digest-remaining-title-wrap" }, [
                  h("div", { class: "digest-remaining-eyebrow" }, [
                    h("span", { class: "digest-remaining-rank-chip" }, `第 ${idx + 4} 篇`),
                    pm.conclusion ? h("span", { class: "digest-remaining-verdict-chip" }, pm.conclusion) : null,
                  ].filter(Boolean)),
                  h("div", { class: "digest-remaining-title" }, row.title || "未命名文章"),
                ]),
                score != null ? h("div", { class: "digest-remaining-score" }, [
                  h("span", {}, "分数"),
                  h("strong", {}, Math.round(Number(score) || 0)),
                ]) : null,
              ].filter(Boolean)),
              sourceMeta ? h("div", { class: "digest-remaining-meta" }, sourceMeta) : null,
              summaryText ? h("p", { class: "digest-remaining-summary" }, summaryText) : null,
              h("div", { class: "digest-remaining-actions" }, [
                h("button", {
                  type: "button",
                  class: "digest-queue-action is-primary",
                  onclick: (event) => {
                    event.stopPropagation();
                    focusDigestNode(detailId, { returnTarget: tocId });
                  },
                }, "看详情"),
                row.url ? h("a", {
                  href: row.url,
                  target: "_blank",
                  rel: "noopener",
                  class: "digest-queue-action",
                  onclick: (event) => event.stopPropagation(),
                }, "原文 ↗") : null,
              ].filter(Boolean)),
            ]),
          ]);
        }));
      };
      const renderRemainingDetails = (rows = []) => rows.length ? h("section", { class: "digest-card digest-card-nested digest-remaining-detail-section" }, [
        h("div", { class: "digest-card-head" }, [h("strong", {}, "其余精选详情")]),
        h("div", { class: "digest-remaining-detail-list" }, rows.map((row, idx) => renderAiArticleCard(row, `第 ${idx + 4} 篇`, {
          compact: false,
          variant: "remaining-detail",
          id: digestRemainingDetailId(row),
          returnTarget: digestRemainingTocId(row),
          returnLabel: "回到其余精选目录",
        }))),
      ]) : null;
      const renderTopicArticles = (rows = []) => rows.length ? h("div", { class: "digest-example-list" }, rows.map((row) => h("article", { class: "digest-example-item" }, [
        h("div", { class: "digest-example-title" }, row.url
          ? h("a", { href: row.url, target: "_blank", rel: "noopener" }, `${row.title || "未命名文章"} ↗`)
          : (row.title || "未命名文章")),
        h("div", { class: "digest-example-meta" }, [row.source_name, row.pub_date ? row.pub_date.slice(0, 10) : null, row.score ? `分数 ${Math.round(Number(row.score) || 0)}` : null].filter(Boolean).join(" · ")),
      ]))) : null;
      const renderTopicCards = (rows = []) => rows.length ? h("div", { class: "digest-platform-list" }, rows.map((row, idx) => h("section", { class: "digest-platform-card" }, [
        h("div", { class: "digest-platform-head" }, [
          h("div", { class: "digest-platform-title" }, `主题 ${row.rank || idx + 1} · ${row.title || "未命名主题"}`),
          h("div", { class: "digest-platform-meta" }, [row.article_count ? `引用 ${row.article_count} 篇` : null, row.score ? `评分 ${Math.round(Number(row.score) || 0)}` : null].filter(Boolean).join(" · ")),
        ]),
        row.why_now ? h("p", { class: "digest-mainline" }, row.why_now) : null,
        row.overview ? h("p", { class: "digest-pill-copy" }, row.overview) : null,
        (row.common_points || []).length ? renderDetailList("共同点", row.common_points) : null,
        (row.differences || []).length ? renderDetailList("差异点", row.differences) : null,
        renderTopicArticles(row.articles || []),
      ].filter(Boolean)))) : null;
      const renderAiPmBriefQueue = (title, rows = []) => {
        if (!rows.length) return null;
        return h("section", { class: "digest-ai-pm-brief-queue" }, [
          h("div", { class: "digest-ai-pm-brief-queue-head" }, [
            h("strong", {}, title),
            h("span", {}, `${rows.length} 篇`),
          ]),
          h("ol", {}, rows.map((row) => {
            const analysis = findDigestAnalysis(row, pmAnalysisMap);
            const targetId = analysis ? digestAnalysisId(analysis) : "";
            const reasonText = uniqueDigestText(
              analysis?.one_line_judgment || row.reason || analysis?.post_analysis_reason || "",
              [row.excerpt, row.summary, row.title],
            );
            const jump = () => {
              if (targetId) focusDigestAnalysis(targetId, digestPmQueueId(row));
              else focusDigestNode("digest-ai-pm", { openDetails: false });
            };
            return h("li", {}, [
              h("button", { type: "button", class: "digest-ai-pm-brief-link", onclick: jump }, row.title || "未命名文章"),
              reasonText ? h("p", {}, reasonText) : null,
            ].filter(Boolean));
          })),
        ]);
      };
      const renderAiPmBriefLead = () => {
        const pmQueuedRows = [...(pmDigest.worth_reading || []), ...(pmDigest.optional_reading || [])];
        const pmQueuedKeys = new Set(pmQueuedRows.map((row) => digestArticleKey(row)));
        const pmExtraAnalyses = pmQueuedRows.length
          ? pmAnalyses.filter((row) => !pmQueuedKeys.has(digestArticleKey(row)))
          : pmAnalyses;
        const hasPmContent = pmDigest.summary
          || (pmDigest.worth_reading || []).length
          || (pmDigest.optional_reading || []).length
          || pmAnalyses.length;
        if (!hasPmContent) return null;
        return h("article", { id: "digest-ai-pm-brief", class: "digest-ai-pm-brief-lead digest-focusable" }, [
          h("div", { class: "digest-ai-pm-brief-badge" }, [
            h("span", {}, "PM"),
            h("small", {}, "深度解读"),
          ]),
          h("div", { class: "digest-ai-pm-brief-main" }, [
            h("div", { class: "digest-brief-card-kicker" }, "先看完整判断"),
            h("h4", {}, "PM 深度解读"),
            pmDigest.summary
              ? h("p", { class: "digest-ai-pm-brief-summary" }, pmDigest.summary)
              : h("p", { class: "digest-ai-pm-brief-summary" }, "今天没有单独的总述，先看下面的 PM 目录和逐篇判断。"),
            h("div", { class: "digest-ai-pm-brief-meta" }, [
              (pmDigest.worth_reading || []).length ? h("span", {}, `值得读 ${(pmDigest.worth_reading || []).length}`) : null,
              (pmDigest.optional_reading || []).length ? h("span", {}, `可选读 ${(pmDigest.optional_reading || []).length}`) : null,
              pmAnalyses.length ? h("span", {}, `逐篇解读 ${pmAnalyses.length}`) : null,
            ].filter(Boolean)),
            ((pmDigest.worth_reading || []).length || (pmDigest.optional_reading || []).length)
              ? h("div", { class: "digest-ai-pm-brief-queues" }, [
                  renderAiPmBriefQueue("值得读", pmDigest.worth_reading || []),
                  renderAiPmBriefQueue("可选读", pmDigest.optional_reading || []),
                ].filter(Boolean))
              : null,
            pmExtraAnalyses.length ? h("div", { class: "digest-ai-pm-brief-conclusions" }, [
              h("div", { class: "digest-ai-pm-brief-conclusions-head" }, [
                h("strong", {}, pmQueuedRows.length ? "补充 PM 判断" : "逐篇 PM 判断"),
                h("span", {}, "点标题可跳到完整卡片"),
              ]),
              h("div", { class: "digest-ai-pm-brief-conclusion-list" }, pmExtraAnalyses.map((row, idx) => {
                const analysisId = digestAnalysisId(row);
                return h("button", {
                  type: "button",
                  class: "digest-ai-pm-brief-conclusion",
                  onclick: () => focusDigestAnalysis(analysisId, "digest-ai-pm-brief"),
                }, [
                  h("span", {}, `#${idx + 1}`),
                  h("strong", {}, row.title || "未命名文章"),
                  row.one_line_judgment ? h("em", {}, row.one_line_judgment) : null,
                ].filter(Boolean));
              })),
            ]) : null,
            h("button", {
              type: "button",
              class: "digest-brief-action",
              onclick: () => focusDigestNode("digest-ai-pm", { openDetails: false }),
            }, "看完整 PM 目录"),
          ].filter(Boolean)),
        ]);
      };
      const renderAiBriefBoard = () => {
        const rows = [];
        (payload.top_picks || []).slice(0, 3).forEach((row, idx) => {
          const pm = row.pm_analysis || {};
          rows.push({
            badge: `Top ${idx + 1}`,
            kicker: "如果今天最懒",
            title: row.title || "未命名文章",
            text: pm.one_line_judgment || row.editorial_reason || row.reason || row.summary || "",
            meta: [
              row.source_name,
              pm.post_analysis_score != null ? `PM ${Math.round(Number(pm.post_analysis_score) || 0)}` : "",
              row.editorial_score != null ? `编辑 ${Math.round(Number(row.editorial_score) || 0)}` : "",
            ].filter(Boolean).join(" · "),
            chips: [...(row.editorial_signals || []), ...(row.keywords || [])].slice(0, 4),
            actionLabel: "跳到 Top 3",
            onClick: () => focusDigestNode("digest-ai-top-picks", { openDetails: false }),
            variant: idx === 0 && !pmAnalyses.length && !pmDigest.summary ? "lead" : "",
            max: idx === 0 ? 150 : 118,
          });
        });
        if ((payload.topics || []).length) {
          rows.push({
            badge: "主题",
            kicker: "看趋势，不看单篇",
            title: "主题追踪",
            text: (payload.topics || []).slice(0, 2).map((row) => row.why_now || row.overview || row.title).filter(Boolean).join("；"),
            meta: `${(payload.topics || []).length} 个主题`,
            chips: (payload.topics || []).slice(0, 4).map((row) => row.title),
            actionLabel: "看主题",
            onClick: () => focusDigestNode("digest-ai-topics", { openDetails: false }),
            max: 140,
          });
        }
        if ((payload.remaining_articles || []).length) {
          rows.push({
            badge: "目录",
            kicker: "其余精选",
            title: "先看标题目录，再点详情",
            text: payload.article_count_note || `还有 ${(payload.remaining_articles || []).length} 篇，不建议一口气读完。先扫目录，挑真正相关的点开。`,
            meta: `${(payload.remaining_articles || []).length} 篇其余精选`,
            chips: (payload.remaining_articles || []).slice(0, 4).map((row) => row.title),
            actionLabel: "看其余精选",
            onClick: () => focusDigestNode("digest-ai-articles", { openDetails: false }),
          });
        }
        return renderDigestBriefBoard(
          "AI 精选日报：先做阅读决策，不急着展开全文",
          "这类日报文章多、PM 解读也多。轻阅读把它拆成四个入口：只看 Top 3、按 PM 判断读、按主题追踪、再挑其余精选。",
          rows,
          {
            variant: "ai",
            eyebrow: "AI DAILY · 轻阅读",
            stats: [
              `精选 ${articleCount} 篇`,
              (payload.top_picks || []).length ? `Top ${(payload.top_picks || []).length}` : "",
              pmAnalyses.length ? `${pmAnalyses.length} 条 PM 解读` : "",
              (payload.topics || []).length ? `${(payload.topics || []).length} 个主题` : "",
            ],
            limit: 7,
            beforeGrid: renderAiPmBriefLead(),
          },
        );
      };
      const renderPmAnalysisArea = () => {
        if (!pmAnalyses.length) return null;
        const block = h("section", { class: "digest-card digest-card-nested digest-pm-analysis-section" }, [
          h("div", { class: "digest-card-head" }, [h("strong", {}, "重点 PM 解读")]),
          renderAiPmRows(pmAnalyses, pmReturnMap),
        ]);
        if (!isDigestBriefMode()) return block;
        return h("details", { class: "digest-brief-disclosure" }, [
          h("summary", { class: "digest-brief-disclosure-summary" }, `展开重点 PM 解读（${pmAnalyses.length} 条）`),
          block,
        ]);
      };
      const renderRemainingDetailsArea = (rows = []) => {
        const block = renderRemainingDetails(rows);
        if (!block || !isDigestBriefMode()) return block;
        return h("details", { class: "digest-brief-disclosure" }, [
          h("summary", { class: "digest-brief-disclosure-summary" }, `展开其余精选详情（${rows.length} 篇）`),
          block,
        ]);
      };
      return h("section", { class: "digest-card digest-ai-daily-shell" }, [
        h("div", { class: "digest-card-head" }, [h("strong", {}, "AI精选日报")]),
        h("div", { class: "digest-top-stats" }, [`精选 ${articleCount} 篇`, payload.total_articles ? `总池子 ${payload.total_articles} 篇` : null].filter(Boolean).join(" · ")),
        notice ? h("p", { class: "digest-section-note digest-ai-fallback-note" }, notice) : null,
        !isDigestBriefMode() && payload.summary ? h("p", { class: "digest-mainline" }, payload.summary) : null,
        isDigestBriefMode() ? renderAiBriefBoard() : null,
        h("div", { class: "digest-ai-layout" }, [
          h("aside", { class: "digest-ai-sidebar" }, [
            h("div", { class: "digest-ai-side-card" }, [
              h("div", { class: "digest-pill-meta" }, "快速跳转"),
              h("a", { class: "digest-ai-side-link", href: "#digest-ai-top-picks" }, "值得读 Top 3"),
              h("a", { class: "digest-ai-side-link", href: "#digest-ai-pm" }, "PM 深度解读"),
              h("a", { class: "digest-ai-side-link", href: "#digest-ai-topics" }, "主题追踪"),
              h("a", { class: "digest-ai-side-link", href: "#digest-ai-articles" }, "其余精选"),
            ]),
          ]),
          h("div", { class: "digest-ai-main" }, [
            (payload.top_picks || []).length ? h("section", { id: "digest-ai-top-picks", class: "digest-card digest-card-nested digest-ai-panel digest-ai-hero" }, [
              h("div", { class: "digest-card-head" }, [h("strong", {}, "值得读 Top 3")]),
              h("p", { class: "digest-pill-meta" }, payload.top_picks_note || "默认先看最值得读的 3 篇。"),
              h("div", { class: "digest-ai-top-picks" }, (payload.top_picks || []).map((row, idx) => renderAiDailyTopPickCard(row, idx))),
            ]) : null,
            (pmDigest.summary || (pmDigest.worth_reading || []).length || (pmDigest.optional_reading || []).length || (pmDigest.analyses || []).length) ? h("section", { id: "digest-ai-pm", class: "digest-card digest-card-nested digest-ai-panel" }, [
              h("div", { class: "digest-card-head" }, [h("strong", {}, "PM 深度解读")]),
              pmDigest.summary ? h("p", { class: "digest-mainline" }, pmDigest.summary) : null,
              h("p", { class: "digest-section-note" }, "先扫下面两组文章，点“看解读”会直接跳到对应 PM 卡片，并自动展开完整分析。"),
              ((pmDigest.worth_reading || []).length || (pmDigest.optional_reading || []).length)
                ? h("div", { class: "digest-ai-pm-queues" }, [
                    renderAiPmQueue("值得读", pmDigest.worth_reading || [], pmAnalysisMap),
                    renderAiPmQueue("可选读", pmDigest.optional_reading || [], pmAnalysisMap),
                  ].filter(Boolean))
                : null,
              renderPmAnalysisArea(),
            ].filter(Boolean)) : null,
            h("section", { id: "digest-ai-topics", class: "digest-card digest-card-nested digest-ai-panel" }, [
              h("div", { class: "digest-card-head" }, [h("strong", {}, "主题追踪")]),
              (payload.topics || []).length ? renderTopicCards(payload.topics || []) : h("div", { class: "runtime-empty" }, "当天没有主题追踪。"),
            ]),
            (payload.remaining_articles || []).length ? h("section", { id: "digest-ai-articles", class: "digest-card digest-card-nested digest-ai-panel" }, [
              h("div", { class: "digest-card-head" }, [h("strong", {}, "其余精选")]),
              payload.article_count_note ? h("div", { class: "digest-top-stats" }, payload.article_count_note) : null,
              h("p", { class: "digest-section-note" }, "先看目录，感兴趣再点“看详情”；详情页里可以一键回到这条目录。"),
              renderRemainingDirectory(payload.remaining_articles || []),
              renderRemainingDetailsArea(payload.remaining_articles || []),
            ]) : null,
          ].filter(Boolean)),
        ]),
      ].filter(Boolean));
    };
    const renderNotionDailyReport = (payload) => {
      if (!payload) return h("section", { class: "digest-card" }, [
        h("div", { class: "digest-card-head" }, [h("strong", {}, "Gorden日报")]),
        h("div", { class: "runtime-empty" }, "当天还没有 Gorden AI 资讯日报。"),
      ]);
      const rows = payload.items || [];
      const notionDate = payload.date || date || "";
      const briefRows = rows.slice(0, 3);
      const notionHeadline = payload.main_line || payload.title || "Gorden日报";
      return h("section", { class: "digest-card digest-notion-shell" }, [
        h("div", { class: "digest-notion-meta" }, [
          h("div", { class: "digest-top-stats" }, [
            "外部日报",
            notionDate,
            payload.item_count ? `${payload.item_count} 条` : null,
          ].filter(Boolean).join(" · ")),
          h("div", { class: "digest-notion-actions" }, [
            payload.page_url ? h("a", {
              href: payload.page_url,
              target: "_blank",
              rel: "noopener",
              class: "digest-brief-action",
            }, "打开 Notion 原页 ↗") : null,
            payload.source_url ? h("a", {
              href: payload.source_url,
              target: "_blank",
              rel: "noopener",
              class: "digest-brief-action is-secondary",
            }, "打开日报总表 ↗") : null,
          ].filter(Boolean)),
        ]),
        h("section", { class: "digest-card digest-card-nested digest-notion-hero" }, [
          h("div", { class: "digest-notion-hero-copy" }, [
            payload.title && payload.title !== notionHeadline ? h("div", { class: "digest-pill-meta" }, payload.title) : null,
            h("h3", { class: "digest-notion-title" }, notionHeadline),
            h("p", { class: "digest-section-note digest-notion-note" }, "这份内容就留在每日日报里，不拆进资讯详情。上面先轻扫 Top 3，下面完整明细也保留。"),
          ].filter(Boolean)),
        ]),
        isDigestBriefMode() && briefRows.length ? h("section", { class: "digest-card digest-card-nested digest-notion-brief" }, [
          h("div", { class: "digest-notion-brief-head" }, [
            h("strong", {}, "轻阅读"),
            h("span", {}, `先看前 ${briefRows.length} 条`),
          ]),
          h("div", { class: "digest-notion-brief-grid" }, briefRows.map((row, idx) => h("article", { class: "digest-notion-brief-item" }, [
            h("span", { class: "digest-notion-brief-rank" }, String(idx + 1)),
            h("div", { class: "digest-notion-brief-copy" }, [
              h("strong", {}, row.title || "未命名条目"),
              row.summary ? h("p", {}, shortDigestText(row.summary, 88)) : null,
            ].filter(Boolean)),
            (row.url || payload.page_url) ? h("a", {
              href: row.url || payload.page_url,
              target: "_blank",
              rel: "noopener",
              class: "digest-brief-action is-secondary",
            }, row.url ? "原文 ↗" : "原页 ↗") : null,
          ].filter(Boolean)))),
        ]) : null,
        h("div", { class: "digest-notion-detail-head" }, [
          h("strong", {}, "完整明细"),
          h("span", {}, `${rows.length} 条 · 列表阅读`),
        ]),
        h("div", { class: "digest-notion-list" }, rows.map((row, idx) => {
          const validRefs = (row.refs || []).filter((ref) => ref?.href);
          return h("article", {
            id: `gorden-item-${row.rank || idx + 1}`,
            class: "digest-card digest-card-nested digest-notion-row",
          }, [
            h("div", { class: "digest-notion-row-rank" }, [
              h("span", { class: "digest-notion-item-rank" }, String(row.rank || idx + 1)),
            ]),
            h("div", { class: "digest-notion-row-main" }, [
              h("div", { class: "digest-notion-row-topline" }, [
                validRefs[0]?.text ? h("span", { class: "digest-notion-card-linkhint" }, shortDigestText(validRefs[0].text, 48)) : null,
                (row.url || payload.page_url) ? h("a", {
                  href: row.url || payload.page_url,
                  target: "_blank",
                  rel: "noopener",
                  class: "digest-brief-action",
                }, row.url ? "原文 ↗" : "原页 ↗") : null,
              ].filter(Boolean)),
              h("h3", { class: "digest-notion-item-title" }, row.title || "未命名条目"),
              row.summary ? h("p", { class: "digest-pill-copy digest-notion-summary" }, row.summary) : null,
              validRefs.length ? h("div", { class: "digest-notion-ref-list" }, validRefs.slice(0, 3).map((ref) => h("a", {
                href: ref.href,
                target: "_blank",
                rel: "noopener",
                class: "digest-queue-action",
              }, `${ref.text || "参考链接"} ↗`))) : null,
            ].filter(Boolean)),
          ]);
        })),
      ].filter(Boolean));
    };
    let aiDailyPayload = digest.ai_daily_digest || null;
    const aiDailyNotice = "";
    const pmMemo = digest.pm_memo || digest.pm_analysis || {};
    const renderLocalBriefBoard = () => {
      const mainJudgment = pmMemo.main_judgment || pmMemo.judgment || pmMemo.main_line || pmMemo.why_it_matters || "";
      const mainDetail = mainJudgment === pmMemo.main_line ? "" : (pmMemo.main_line || pmMemo.why_it_matters || "");
      const quickRows = [];
      (digest.top5 || []).slice(0, 3).forEach((row, idx) => {
        quickRows.push({
          badge: `热点${idx + 1}`,
          kicker: "热点地图",
          title: row.title || "未命名热点",
          text: row.what_happened || row.why_hot || "",
          meta: [
            row.mention_count ? `提到 ${row.mention_count} 次` : "",
            row.story_count ? `聚合 ${row.story_count} 条` : "",
          ].filter(Boolean).join(" · "),
          chips: [...(row.related_entities || []), ...(row.related_topics || [])].slice(0, 4),
          actionLabel: "看热点",
          onClick: () => focusDigestNode("digest-local-top5", { openDetails: false }),
        });
      });
      const watchlist = pmMemo.watchlist || [];
      if (watchlist.length) {
        quickRows.push({
          badge: "盯",
          kicker: "后续观察",
          title: "接下来要盯什么",
          text: watchlist.slice(0, 4).join("；"),
          chips: watchlist.slice(0, 3),
          actionLabel: "看观察清单",
          onClick: () => focusDigestNode("digest-local-pm", { openDetails: false }),
        });
      }
      if (!quickRows.length && (digest.pm_insights || []).length) {
        (digest.pm_insights || []).slice(0, 4).forEach((row, idx) => quickRows.push({
          badge: `洞察${idx + 1}`,
          kicker: "PM 洞察",
          title: row.cluster_key || "未命名主题",
          text: row.one_liner || row.pm_takeaway || row.why_it_matters || "",
          actionLabel: "看洞察",
          onClick: () => focusDigestNode("digest-local-pm", { openDetails: false }),
        }));
      }
      if (!quickRows.length) {
        [...(digest.hot_entities || []).slice(0, 3), ...(digest.hot_topics || []).slice(0, 3)].filter(Boolean).forEach((value, idx) => quickRows.push({
          badge: idx < 3 ? "实体" : "主题",
          kicker: "热度信号",
          title: typeof value === "string" ? value : (value.name || value.title || "热点"),
          text: typeof value === "string" ? "" : (value.summary || value.reason || ""),
        }));
      }
      return h("section", { class: "digest-brief-board is-local digest-local-brief-board" }, [
        h("div", { class: "digest-brief-board-head" }, [
          h("div", { class: "panel-kicker" }, "LOCAL DIGEST · 轻阅读"),
          h("div", { class: "digest-brief-board-titleline" }, [
            h("h3", {}, "本地 Digest：先看今日主线，再决定追哪些热点"),
            h("div", { class: "digest-brief-stats" }, [
              (digest.top5 || []).length ? h("span", {}, `${(digest.top5 || []).length} 个热点`) : null,
              (digest.platform_hotspots || []).length ? h("span", {}, `${(digest.platform_hotspots || []).length} 个平台`) : null,
              watchlist.length ? h("span", {}, `${watchlist.length} 个观察点`) : null,
            ].filter(Boolean)),
          ]),
          h("p", {}, "这类日报先给结论更重要：你先知道今天主线，再去看热点列表和后续观察。"),
        ]),
        h("article", { class: "digest-local-mainline-card" }, [
          h("div", { class: "digest-local-mainline-label" }, "今日主线"),
          h("div", { class: "digest-local-mainline-body" }, [
            h("strong", {}, mainJudgment || "今天还没有形成清晰主线"),
            mainDetail ? h("p", {}, shortDigestText(mainDetail, 220)) : null,
            h("div", { class: "digest-local-mainline-actions" }, [
              h("button", { type: "button", class: "digest-brief-action", onclick: () => focusDigestNode("digest-local-pm", { openDetails: false }) }, "看 PM Memo"),
              h("button", { type: "button", class: "digest-brief-action is-secondary", onclick: () => focusDigestNode("digest-local-top5", { openDetails: false }) }, "看热点 Top"),
            ]),
          ].filter(Boolean)),
        ]),
        quickRows.length ? h("div", { class: "digest-brief-grid digest-local-quick-grid" }, quickRows.slice(0, 6).map((row, idx) => {
          const action = row.onClick ? h("button", { type: "button", class: "digest-brief-action", onclick: row.onClick }, row.actionLabel || "看详情") : null;
          return h("article", { class: "digest-brief-card" }, [
            h("div", { class: "digest-brief-card-badge" }, row.badge || String(idx + 1)),
            h("div", { class: "digest-brief-card-main" }, [
              row.kicker ? h("div", { class: "digest-brief-card-kicker" }, row.kicker) : null,
              h("strong", {}, row.title || "未命名"),
              row.text ? h("p", {}, shortDigestText(row.text, 105)) : null,
              row.meta ? h("div", { class: "digest-brief-meta" }, row.meta) : null,
              (row.chips || []).length ? h("div", { class: "digest-brief-chips" }, (row.chips || []).slice(0, 3).map((chip) => h("span", {}, shortDigestText(chip, 26)))) : null,
              action,
            ].filter(Boolean)),
          ]);
        })) : null,
      ].filter(Boolean));
    };
    const renderLocalDigest = () => {
      if ((digest.top5 || []).length || pmMemo.main_judgment || pmMemo.judgment) {
        const pmCard = h("section", { id: "digest-local-pm", class: "digest-card digest-focusable" }, [
          h("div", { class: "digest-card-head" }, [h("strong", {}, "PM Memo")]),
          h("div", { class: "digest-insight-list" }, [
            h("article", { class: "digest-insight-card" }, [
              (pmMemo.main_judgment || pmMemo.judgment) ? h("div", { class: "digest-insight-title" }, pmMemo.main_judgment || pmMemo.judgment) : null,
              pmMemo.main_line ? h("p", { class: "digest-mainline" }, pmMemo.main_line) : (pmMemo.why_it_matters ? h("p", { class: "digest-mainline" }, pmMemo.why_it_matters) : null),
              (pmMemo.hotspot_analysis || []).length ? h("div", { class: "digest-hotspot-analysis" }, [
                h("strong", {}, "这几个热点真正说明什么"),
                h("div", { class: "digest-hotspot-list" }, (pmMemo.hotspot_analysis || []).map((row) => h("article", { class: "digest-hotspot-item" }, [
                  h("div", { class: "digest-hotspot-title" }, row.title || "未命名热点"),
                  row.analysis ? h("p", {}, row.analysis) : null,
                ]))),
              ]) : null,
              (pmMemo.pm_implications || []).length ? h("div", {}, [h("strong", {}, "对产品经理意味着什么"), renderBulletList(pmMemo.pm_implications)]) : null,
              (pmMemo.watchlist || []).length ? h("div", {}, [h("strong", {}, "接下来要盯"), renderBulletList(pmMemo.watchlist)]) : null,
            ]),
          ]),
        ]);
        return [renderModelTop5(digest.top5 || [], "昨日热点"), renderPlatformHotspots(digest.platform_hotspots || []), pmCard];
      }
      const insightSection = h("section", { id: "digest-local-pm", class: "digest-card digest-focusable" }, [
        h("div", { class: "digest-card-head" }, [h("strong", {}, "必读 / 值得读 PM 洞察")]),
        h("div", { class: "digest-insight-list" }, (digest.pm_insights || []).map((row) => h("article", { class: "digest-insight-card" }, [
          h("div", { class: "digest-insight-title" }, row.cluster_key || "未命名主题"),
          h("p", {}, row.one_liner || ""),
          h("p", {}, row.why_it_matters || ""),
          h("p", {}, row.pm_takeaway || ""),
          (row.follow_up_questions || []).length ? h("ul", { class: "digest-news-list" }, row.follow_up_questions.map((q) => h("li", {}, q))) : null,
        ]))),
      ]);
      return [renderList(digest.hot_entities || [], "昨日热点实体"), renderList(digest.hot_topics || [], "昨日热点主题"), insightSection];
    };
    if (state.activeDigestTab === "ai-daily") {
      $("#digest-title").textContent = `AI精选日报 · ${digestIssueDate(date, "ai-daily")}`;
      $("#digest-subtitle").textContent = isAiDailyPayloadEmpty(aiDailyPayload)
        ? "这一天没有 AI 精选日报数据，不再自动借用其它日期。"
        : `精选 ${(aiDailyPayload?.selected_count || aiDailyPayload?.export_meta?.article_count || (aiDailyPayload?.articles || []).length || 0)} 篇，按源头当天数据展示。`;
      content.append(renderAiDailyReport(aiDailyPayload, aiDailyNotice));
      return;
    }
    if (state.activeDigestTab === "gorden-daily") {
      $("#digest-title").textContent = `Gorden日报 · ${digestIssueDate(date, "gorden-daily")}`;
      $("#digest-subtitle").textContent = (digest.notion_daily_digest?.item_count || 0)
        ? `共 ${digest.notion_daily_digest.item_count} 条，保留日报式浏览。`
        : "这一天还没有导入 Gorden AI 资讯日报。";
      content.append(renderNotionDailyReport(digest.notion_daily_digest || null));
      return;
    }
    const localSections = renderLocalDigest().filter(Boolean);
    content.append(...(isDigestBriefMode() ? [renderLocalBriefBoard(), ...localSections] : localSections));
  } catch (err) {
    content.innerHTML = `<div class="runtime-empty">${err.message || "读取 digest 失败"}</div>`;
  }
}

async function jumpToDay(date) {
  if (state.activeView !== "feed") toggleDigestView("feed");
  const idx = state.index.days.findIndex(d => d.date === date);
  if (idx < 0) return;
  state.timelineCount = Math.max(state.timelineCount || 1, 1);
  const unlockingCurrentDay = state.selectedDate === date;
  state.timelineAutoLoadDisabledUntil = Date.now() + 900;
  if (unlockingCurrentDay) {
    state.selectedDate = null;
    clearDayQuickFilters();
  } else {
    state.selectedDate = date;
    setTimelineWindowForDate(date);
    if (state.dayQuickFilters?.date !== date) clearDayQuickFilters();
  }
  renderSidebar();
  await renderTimeline();
  const targetDate = state.selectedDate || date;
  const el = [...$$(".day")].find(n => n.getAttribute("data-date") === targetDate);
  if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
}

function syncSidebarDateState(fallbackDate = "") {
  const activeDate = state.selectedDate || "";
  $$(".date-item").forEach((el) => {
    el.classList.toggle("active", el.getAttribute("data-date") === activeDate);
  });
}

function filterGlobalItems(items) {
  return items.filter((it) => {
    if (state.activeCategories.size && !state.activeCategories.has(it.category)) return false;
    if (state.activeSources.size && !state.activeSources.has(it.source)) return false;
    if (state.activeEntityTags.size) {
      const entityTags = new Set(it.entity_tags || []);
      const matchEntity = [...state.activeEntityTags].some((tag) => entityTags.has(tag));
      if (!matchEntity) return false;
    }
    if (state.activeTopicTags.size) {
      const topicTags = new Set(it.topic_tags || []);
      const matchTopic = [...state.activeTopicTags].some((tag) => topicTags.has(tag));
      if (!matchTopic) return false;
    }
    const label = getItemScoreLabel(it);
    if (state.activeScoreLabels.size && !state.activeScoreLabels.has(label)) return false;
    if (state.activeItemStates.size) {
      const itemState = getItemState(it.item_id);
      const matchesSomeState = [...state.activeItemStates].some((key) => (key === "unread" ? !itemState.read : Boolean(itemState[key])));
      if (!matchesSomeState) return false;
    }
    for (const dimension of SCORE_DIMENSIONS) {
      const selected = state.activeLevels[dimension.key];
      if (selected?.size && !selected.has(it[dimension.field])) return false;
    }
    if (state.search) {
      const hay = [
        it.title, it.summary, it.author, it.source, it.category, it.pm_reason, it.pm_label,
        it.pm_signal_level, it.pm_decision_level, it.pm_transfer_level, it.pm_evidence_level, it.pm_constraint_level,
        ...(it.entity_tags || []), ...(it.topic_tags || []), it.tag_reason, getItemState(it.item_id).note,
      ].filter(Boolean).join(" ").toLowerCase();
      if (!hay.includes(state.search)) return false;
    }
    return true;
  });
}

function applyDayQuickFilters(items, date) {
  return items;
}

function filterItems(items, date = null) {
  return applyDayQuickFilters(filterGlobalItems(items), date);
}

function renderDay(dMeta, day, items, baseItems = items) {
  const [y, m, d] = dMeta.date.split("-").map(Number);
  const dateObj = new Date(Date.UTC(y, m - 1, d));
  const weekday = WEEKDAYS[dateObj.getUTCDay()];
  const monthAbbr = MONTHS[m - 1];

  // Fold clusters before display: replace multi-member clusters with a single
  // "rep" item carrying a .variants array.
  const folded = foldClusters(day, items);
  const categoryGroups = getDayCategoryGroups(folded);

  const body = h("div", { class: "day-body" });

  if (state.settings.group === "flat") {
    const sorted = [...folded].sort(bySegmentDesc);
    body.appendChild(renderItems(sorted, {
      initialCount: FEED_FLAT_INITIAL_COUNT,
      batchSize: FEED_FLAT_BATCH_COUNT,
      buttonLabel: "继续加载",
      date: dMeta.date,
      groupName: "__flat__",
    }));
  } else if (state.settings.group === "source") {
    const groups = bucket(folded, (it) => it.source || "—");
    for (const [name, its] of sortedGroups(groups, null)) {
      body.appendChild(renderGroup(name, its, true, "", dMeta.date));
    }
  } else {
    for (const [name, its] of categoryGroups) {
      body.appendChild(renderGroup(name, its, false, "", dMeta.date));
    }
  }

  return h("div", { class: "day", "data-date": dMeta.date }, [
    h("div", { class: "day-dot" }),
    renderDayRail(dMeta, d, monthAbbr, y, weekday, renderDayStats(items, folded, dMeta)),
    body,
  ]);
}

function renderDayStats(visibleItems, foldedItems, dMeta) {
  const folded = visibleItems.length - foldedItems.length;
  if (folded > 0) return `${foldedItems.length} / ${dMeta.items} · −${folded} DUP`;
  return `${visibleItems.length} / ${dMeta.items} ITEMS`;
}

function renderEmptyDay(dMeta, message, baseItems = []) {
  const [y, m, d] = dMeta.date.split("-").map(Number);
  const dateObj = new Date(Date.UTC(y, m - 1, d));
  const weekday = WEEKDAYS[dateObj.getUTCDay()];
  const monthAbbr = MONTHS[m - 1];
  const categoryGroups = getDayCategoryGroups(baseItems);
  return h("div", { class: "day day-empty", "data-date": dMeta.date }, [
    h("div", { class: "day-dot" }),
    renderDayRail(dMeta, d, monthAbbr, y, weekday, "0 ITEMS"),
    h("div", { class: "day-body" }, [
      h("div", { class: "empty-day" }, [
        h("div", { class: "empty-day-title" }, "当前日期没有匹配结果"),
        h("div", { class: "empty-day-copy" }, message || "请调整筛选条件后重试。"),
      ]),
    ]),
  ]);
}

function renderDayRail(dMeta, dayNumber, monthAbbr, year, weekday, stats) {
  return h("div", { class: "day-rail" }, [
    h("div", { class: "day-rail-card" }, [
      h("span", { class: "day-num" }, String(dayNumber).padStart(2, "0")),
      h("span", { class: "day-month" }, `${monthAbbr} ${year}`),
      h("span", { class: "day-weekday" }, weekday),
      h("span", { class: "day-stats" }, stats),
    ]),
  ]);
}

function getDayCategoryGroups(items) {
  const groups = bucket(items, (it) => it.category || "📦 其他");
  const order = (state.index?.categories || []).map(c => c.label);
  return sortedGroups(groups, order);
}

// Apply the pre-computed clustering to the (already-filtered) item list.
// For each cluster: if >=2 of its members survive filtering, replace them
// with a single synthetic item (the representative) whose `.variants` is the
// remaining members.
function foldClusters(day, filteredItems) {
  const profile = state.settings.cluster;
  if (profile === "off") return filteredItems;
  const clusters = (day.clusters && day.clusters[profile]) || [];
  if (!clusters.length) return filteredItems;

  const filteredSet = new Set(filteredItems.map(it => it.item_id));
  const consumed = new Set();
  const extras = new Map(); // rep item_id -> variant items

  for (const c of clusters) {
    const members = c.members
      .map(i => day.items[i])
      .filter(it => it && filteredSet.has(it.item_id));
    if (members.length < 2) continue;
    const rep = day.items[c.rep];
    if (!rep || !filteredSet.has(rep.item_id)) continue;
    for (const m of members) if (m.item_id !== rep.item_id) consumed.add(m.item_id);
    extras.set(rep.item_id, members.filter(m => m.item_id !== rep.item_id));
  }

  const out = [];
  for (const it of filteredItems) {
    if (consumed.has(it.item_id)) continue;
    const variants = extras.get(it.item_id);
    if (variants && variants.length) {
      out.push({ ...it, variants });
    } else {
      out.push(it);
    }
  }
  return out;
}

function renderGroup(name, items, italicize, id = "", date = "") {
  const sorted = [...items].sort(bySegmentDesc);
  const attrs = { class: "cat-group" };
  if (id) attrs.id = id;
  return h("section", attrs, [
    h("div", { class: "cat-head" }, [
      h("span", { class: "cat-title" }, name),
      h("span", { class: "cat-count" }, String(sorted.length).padStart(2, "0")),
      h("span", { class: "cat-rule" }),
    ]),
    renderItems(sorted, {
      initialCount: FEED_GROUP_INITIAL_COUNT,
      batchSize: FEED_GROUP_BATCH_COUNT,
      buttonLabel: italicize ? "继续展开" : "继续看",
      date,
      groupName: name,
    }),
  ]);
}

function renderItems(items, opts = {}) {
  const wrap = h("div", { class: "items" });
  const total = Array.isArray(items) ? items.length : 0;
  const renderKey = (opts.date || opts.groupName) ? [opts.date || "", opts.groupName || "", state.settings.group || ""].join("::") : "";
  const rememberedCount = renderKey ? Number(state.itemRenderCounts.get(renderKey) || 0) : 0;
  const initialCount = Math.min(total, Math.max(0, Number(opts.initialCount) || 0, rememberedCount));
  const batchSize = Math.max(1, Number(opts.batchSize) || initialCount || total || 1);

  const appendRange = (from, to) => {
    const frag = document.createDocumentFragment();
    for (let idx = from; idx < to; idx += 1) frag.appendChild(renderItem(items[idx]));
    wrap.appendChild(frag);
  };

  if (!initialCount || total <= initialCount) {
    appendRange(0, total);
    return wrap;
  }

  const shell = h("div", { class: "items-shell" });
  const footer = h("div", { class: "items-loadmore" });
  const button = h("button", { type: "button", class: "items-loadmore-btn" });
  const meta = h("span", { class: "items-loadmore-meta" });
  const actionLabel = String(opts.buttonLabel || "继续加载").trim() || "继续加载";
  let rendered = 0;
  let autoObserver = null;

  const syncFooter = () => {
    if (rendered >= total) {
      if (autoObserver) autoObserver.disconnect();
      footer.remove();
      return;
    }
    const remaining = total - rendered;
    button.textContent = `${actionLabel} ${Math.min(batchSize, remaining)} 条`;
    meta.textContent = `已展示 ${rendered} / ${total}`;
  };

  const renderNext = (count) => {
    const next = Math.min(total, rendered + count);
    appendRange(rendered, next);
    rendered = next;
    if (renderKey) state.itemRenderCounts.set(renderKey, rendered);
    syncFooter();
  };

  button.addEventListener("click", () => renderNext(batchSize));
  footer.append(button, meta);
  shell.append(wrap, footer);
  renderNext(initialCount);
  autoObserver = observeAutoLoadMore(footer, () => renderNext(batchSize), AUTO_ITEM_LOAD_ROOT_MARGIN);
  return shell;
}

function renderItem(it) {
  const title = cleanTitle(it.title);
  const itemState = getItemState(it.item_id);
  const titleNode = it.url
    ? h("a", { href: it.url, target: "_blank", rel: "noopener" }, [title, h("span", { class: "arrow" }, "↗")])
    : h("span", {}, title);

  const contextNodes = [];
  const author = sanitizeAuthor(it.author);
  const priorityClass = ({ "必读": "item-read-essential", "值得读": "item-read-recommended" })[it.pm_label] || "";

  if (it.category) contextNodes.push(renderItemContext("分类", it.category, "category"));
  if (it.source) contextNodes.push(renderItemContext("渠道", it.source, "source"));
  if (author) contextNodes.push(renderItemContext("作者", author, "author"));

  const kids = [
    h("div", { class: "title" }, [renderScoreBadge(it), titleNode]),
    contextNodes.length ? h("div", { class: "item-context-row" }, contextNodes) : null,
  ];
  kids.push(renderItemActions(it, itemState));
  if (it.summary) kids.push(h("div", { class: "summary", html: linkify(it.summary) }));
  if (it.pm_reason) kids.push(h("div", { class: "score-reason" }, it.pm_reason));
  if ((it.entity_tags || []).length || (it.topic_tags || []).length || it.tag_status === "pending" || it.tag_status === "error") {
    kids.push(renderTagPills(it));
  }
  const levelBadges = renderLevelBadges(it);
  if (levelBadges) kids.push(levelBadges);

  if (it.variants && it.variants.length) {
    const srcCounts = {};
    for (const v of it.variants) srcCounts[v.source || "?"] = (srcCounts[v.source || "?"] || 0) + 1;
    srcCounts[it.source || "?"] = (srcCounts[it.source || "?"] || 0) + 1;
    const nSrc = Object.keys(srcCounts).length;
    const total = it.variants.length + 1;
    const badge = h("summary", { class: "cluster-badge", title: "展开相似条目" }, [h("span", { class: "cluster-count" }, `+${it.variants.length}`), h("span", { class: "dot" }), h("span", {}, `${total}× · ${nSrc} ${nSrc === 1 ? "source" : "sources"}`), h("span", { class: "chev" }, "▾")]);
    const variantList = h("ol", { class: "variants" }, it.variants.slice().sort(bySegmentDesc).map(renderVariant));
    kids.push(h("details", { class: "cluster-details" }, [badge, variantList]));
  }

  return h("article", { class: ["item", it.variants ? "has-cluster" : "", priorityClass, itemState.read ? "item-state-read" : "", itemState.starred ? "item-state-starred" : ""].filter(Boolean).join(" ") }, kids);
}

function renderItemContext(label, value, kind) {
  return h("span", { class: `item-context-part item-context-${kind}`, title: `${label}：${value}` }, [
    h("span", { class: "item-context-label" }, label),
    h("span", { class: "item-context-value" }, value),
  ]);
}

function renderScoreBadge(it) {
  if (it.pm_label) return h("span", { class: `score-pill title-score score-${slugify(it.pm_label)}` }, `${it.pm_label}${it.pm_score != null ? ` ${it.pm_score}` : ""}`);
  if (it.pm_score_status === "pending") return h("span", { class: "score-pill title-score score-pending" }, "评分中");
  if (it.pm_score_status === "error") return h("span", { class: "score-pill title-score score-error" }, "评分失败");
  return h("span", { class: "score-pill title-score score-unscored" }, "未评分");
}

function formatItemSegment(segment) {
  const raw = String(segment || "").trim();
  if (!raw || raw.toUpperCase() === "RSS" || raw.toUpperCase() === "AIHOT") return "";
  return raw
    .replace(/^(\d{2})-(\d{2})$/, "$1–$2")
    .replace(/\s*→\s*26\/\d{2}\/\d{2}\s*/g, " → ");
}

function renderItemActions(it, itemState) {
  if (!it.item_id) return null;
  const actions = [
    {
      key: "starred",
      icon: "★",
      label: itemState.starred ? "已收藏" : "收藏",
      active: itemState.starred,
      patch: () => ({ starred: !itemState.starred }),
    },
    {
      key: "read_later",
      icon: "⏱",
      label: itemState.read_later ? "已稍后读" : "稍后读",
      active: itemState.read_later,
      patch: () => ({ read_later: !itemState.read_later }),
    },
    {
      key: "read",
      icon: "✓",
      label: itemState.read ? "已读" : "标记已读",
      active: itemState.read,
      patch: () => ({ read: !itemState.read, read_later: itemState.read ? itemState.read_later : false }),
    },
  ];
  return h("div", { class: "item-actions" }, actions.map((action) => h("button", {
    class: `item-action ${action.active ? "active" : ""}`,
    type: "button",
    "data-kind": action.key,
    "aria-pressed": action.active ? "true" : "false",
    title: action.label,
    onclick: async (event) => {
      event.preventDefault();
      event.stopPropagation();
      try {
        await updateItemState(it.item_id, action.patch());
        renderSidebar();
        await renderTimeline();
      } catch (err) {
        setRefreshStatus("error", err.message || "状态更新失败");
        window.setTimeout(() => setRefreshStatus(), 3200);
      }
    },
  }, [
    h("span", { class: "item-action-icon", "aria-hidden": "true" }, action.icon),
    h("span", { class: "item-action-label" }, action.label),
  ])));
}

function renderTagPills(it) {
  const rows = [];
  for (const tag of (it.entity_tags || []).slice(0, 3)) rows.push(h("span", { class: "tag-pill tag-pill-entity" }, tag));
  for (const tag of (it.topic_tags || []).slice(0, 3)) rows.push(h("span", { class: "tag-pill tag-pill-topic" }, tag));
  if (!rows.length && it.tag_status === "pending") rows.push(h("span", { class: "tag-pill tag-pill-pending" }, "标签中"));
  if (!rows.length && it.tag_status === "error") rows.push(h("span", { class: "tag-pill tag-pill-error" }, "标签失败"));
  return h("div", { class: "tag-row" }, rows);
}

function renderLevelBadges(it) {
  const parts = [
    ["S", it.pm_signal_level],
    ["D", it.pm_decision_level],
    ["T", it.pm_transfer_level],
    ["E", it.pm_evidence_level],
    ["C", it.pm_constraint_level],
  ].filter(([, value]) => Boolean(value));
  if (!parts.length) return null;
  return h("div", { class: "level-row" }, parts.map(([abbr, value]) =>
    h("span", { class: `level-pill level-${value}` }, `${abbr}:${value}`)
  ));
}

function renderVariant(v) {
  const titleEl = v.url
    ? h("a", { href: v.url, target: "_blank", rel: "noopener", class: "variant-title" },
        [cleanTitle(v.title), h("span", { class: "arrow" }, "↗")])
    : h("span", { class: "variant-title" }, cleanTitle(v.title));
  const meta = [];
  if (v.source) meta.push(h("span", { class: "source" }, v.source));
  const vAuthor = sanitizeAuthor(v.author);
  if (vAuthor) {
    if (meta.length) meta.push(h("span", { class: "dot" }));
    meta.push(h("span", { class: "author" }, vAuthor));
  }
  if (v.segment) {
    if (meta.length) meta.push(h("span", { class: "dot" }));
    meta.push(h("span", {}, v.segment));
  }
  const kids = [
    h("div", { class: "variant-head" }, [titleEl, h("span", { class: "variant-meta" }, meta)]),
  ];
  if (v.summary) kids.push(h("div", { class: "variant-summary", html: linkify(v.summary) }));
  return h("li", { class: "variant" }, kids);
}

function linkify(text) {
  const safe = text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  // `[text](url)` then bold `**text**`
  return safe
    .replace(/\[([^\]]+)\]\((https?:[^)\s]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>')
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
}

// ─────────────────────────── Utils ───────────────────────────
function segmentSortValue(segment) {
  const raw = String(segment || "").trim();
  if (!raw) return -1;
  if (raw.includes("→")) {
    const tail = raw.split("→").pop()?.trim() || raw;
    const tailNumbers = [...tail.matchAll(/(\d{1,2})/g)].map((match) => Number(match[1]));
    return tailNumbers.length ? tailNumbers[tailNumbers.length - 1] : -1;
  }
  const numbers = [...raw.matchAll(/(\d{1,2})/g)].map((match) => Number(match[1]));
  if (!numbers.length) return -1;
  if (raw.includes("-") && numbers.length >= 2) return numbers[1];
  return numbers[numbers.length - 1];
}

function bySegmentDesc(a, b) {
  // Normal ranges like "22-23" use the later hour (23).
  // Cross-midnight shapes like "23 → 26/04/20 00" also use the latter timestamp (00),
  // so they render at the earliest slot of that day instead of floating to the top.
  const sa = segmentSortValue(a.segment);
  const sb = segmentSortValue(b.segment);
  if (sb !== sa) return sb - sa;
  return String(a.title || a.item_id || "").localeCompare(String(b.title || b.item_id || ""), "zh-CN");
}
function bucket(arr, fn) {
  const m = new Map();
  for (const x of arr) {
    const k = fn(x);
    if (!m.has(k)) m.set(k, []);
    m.get(k).push(x);
  }
  return m;
}
function sortedGroups(map, orderList) {
  const keys = [...map.keys()];
  if (orderList && orderList.length) {
    keys.sort((a, b) => {
      const ia = orderList.indexOf(a); const ib = orderList.indexOf(b);
      return (ia < 0 ? 999 : ia) - (ib < 0 ? 999 : ib);
    });
  } else {
    keys.sort((a, b) => map.get(b).length - map.get(a).length);
  }
  return keys.map(k => [k, map.get(k)]);
}

function slugify(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/[^\w\u4e00-\u9fff-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

main();
