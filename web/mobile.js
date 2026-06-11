const state = {
  dates: [],
  date: null,
  digest: null,
  day: null,
  filter: "must",
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

function text(value, fallback = "") {
  return String(value || fallback || "").trim();
}

function stripHtml(value) {
  const container = document.createElement("div");
  container.innerHTML = text(value);
  return container.textContent || container.innerText || "";
}

function cleanCopy(value, max = 220) {
  let copy = stripHtml(value)
    .replace(/\*\*(.+?)\*\*/g, "$1")
    .replace(/\[([^\]]+)\]\((https?:\/\/[^)\s]+)\)/g, "$1")
    .replace(/\[([^\]]+)\]\(https?:\/\/[^)\s]*/g, "$1")
    .replace(/\s+/g, " ")
    .trim();
  if (copy.length > max) copy = `${copy.slice(0, max - 1)}…`;
  return copy;
}

function formatDate(date) {
  if (!date) return "未知日期";
  const parsed = new Date(`${date}T12:00:00+08:00`);
  return new Intl.DateTimeFormat("zh-CN", {
    month: "long",
    day: "numeric",
    weekday: "short",
  }).format(parsed);
}

function relativeDate(date) {
  const parsed = new Date(`${date}T00:00:00+08:00`);
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const diffDays = Math.round((parsed - today) / 86400000);
  if (diffDays === -1) return "昨日";
  if (diffDays === 0) return "今日";
  return formatDate(date);
}

async function fetchJson(path) {
  const response = await fetch(`${path}?v=${Date.now()}`);
  if (!response.ok) throw new Error(`${path} HTTP ${response.status}`);
  return response.json();
}

async function boot() {
  try {
    const index = await loadDateIndex();
    state.dates = (index.dates || []).filter(Boolean);
    const requestedDate = new URLSearchParams(window.location.search).get("date");
    state.date = state.dates.includes(requestedDate) ? requestedDate : state.dates[0] || null;
    if (!state.date) throw new Error("没有可用日报");
    wireControls();
    await loadDate(state.date);
  } catch (error) {
    renderError(error);
  }
}

async function loadDate(date) {
  state.date = date;
  const [digest, day] = await Promise.all([
    fetchJson(`data/digest/${encodeURIComponent(date)}.json`).catch(() => null),
    fetchJson(`data/day/${encodeURIComponent(date)}.json`).catch(() => ({ date, items: [] })),
  ]);
  state.digest = digest;
  state.day = day;
  syncUrlDate();
  render();
}

async function loadDateIndex() {
  const digestIndex = await fetchJson("data/digest/index.json").catch(() => null);
  if (digestIndex?.dates?.length) return digestIndex;
  const publicIndex = await fetchJson("data/index.json");
  return { dates: (publicIndex.days || []).map((day) => day.date).filter(Boolean) };
}

function wireControls() {
  $("#prev-date").addEventListener("click", () => shiftDate(1));
  $("#next-date").addEventListener("click", () => shiftDate(-1));
  $("#date-pill").addEventListener("click", () => {
    const current = state.dates.indexOf(state.date);
    const next = state.dates[(current + 1) % state.dates.length];
    if (next) loadDate(next);
  });
  $("#jump-recommend").addEventListener("click", () => {
    $("#recommend-title").scrollIntoView({ behavior: "smooth", block: "start" });
  });
  $$(".filter-chip").forEach((button) => {
    button.addEventListener("click", () => {
      state.filter = button.dataset.filter || "must";
      $$(".filter-chip").forEach((chip) => chip.classList.toggle("active", chip === button));
      renderRecommendations();
    });
  });
}

function shiftDate(offset) {
  const index = state.dates.indexOf(state.date);
  const nextIndex = index + offset;
  const next = state.dates[nextIndex];
  if (next) loadDate(next);
}

function render() {
  renderHero();
  renderDateRail();
  renderStories();
  renderRecommendations();
  syncDateButtons();
}

function renderHero() {
  const digest = state.digest || {};
  const memo = digest.pm_memo || digest.pm_analysis || {};
  const items = state.day?.items || [];
  const mustCount = items.filter((item) => item.pm_label === "必读").length;
  const worthCount = items.filter((item) => item.pm_label === "值得读").length;
  const fallbackLine = publicDailyLine(items);
  const judgment = memo.main_judgment || memo.main_line || memo.why_it_matters || digest.top5?.[0]?.why_hot || fallbackLine;

  $("#date-pill").textContent = `${relativeDate(state.date)} · ${state.date}`;
  $("#freshness").textContent = `${formatDate(state.date)} 日报`;
  $("#main-judgment").textContent = `${relativeDate(state.date)} AI 主线`;
  $("#main-line").textContent = cleanCopy(judgment, 280);
  $("#metric-items").textContent = digest.coverage?.items_total || items.length || "--";
  $("#metric-must").textContent = mustCount || "--";
  $("#metric-worth").textContent = worthCount || "--";
  $("#desktop-date-link").href = `index.html?date=${encodeURIComponent(state.date)}`;
}

function renderStories() {
  const mount = $("#story-list");
  const template = $("#story-template");
  mount.innerHTML = "";
  const stories = state.digest?.top5 || publicStories(state.day?.items || []);
  if (!stories.length) {
    mount.appendChild(note("这一天还没有日报主线。"));
    return;
  }
  stories.forEach((story, index) => {
    const node = template.content.firstElementChild.cloneNode(true);
    $(".story-rank", node).textContent = index + 1;
    const title = $(".story-title", node);
    title.textContent = text(story.title, "未命名主线");
    title.href = firstStoryUrl(story) || story.url || `index.html?date=${encodeURIComponent(state.date)}`;
    $(".story-what", node).textContent = cleanCopy(story.what_happened || story.summary, 230);
    $(".story-why", node).textContent = cleanCopy(story.why_hot || story.pm_reason, 250);
    const tags = [...(story.related_entities || []), ...(story.related_topics || [])].slice(0, 5);
    $(".tag-row", node).append(...tags.map((tag) => {
      const el = document.createElement("span");
      el.textContent = tag;
      return el;
    }));
    mount.appendChild(node);
  });
}

function renderDateRail() {
  const rail = $("#date-rail");
  rail.innerHTML = "";
  const current = state.dates.indexOf(state.date);
  const start = Math.max(0, current - 2);
  const dates = state.dates.slice(start, start + 10);
  if (!dates.includes(state.date)) dates.unshift(state.date);
  for (const date of dates) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = date === state.date ? "active" : "";
    button.textContent = shortDate(date);
    button.addEventListener("click", () => loadDate(date));
    rail.appendChild(button);
  }
}

function renderRecommendations() {
  const mount = $("#recommend-list");
  const template = $("#item-template");
  mount.innerHTML = "";
  const items = recommendedItems();
  $("#recommend-title").textContent = state.filter === "must"
    ? "昨日必读"
    : state.filter === "worth"
      ? "值得读推荐"
      : "全部高价值推荐";
  if (!items.length) {
    mount.appendChild(note("当前筛选下没有推荐条目。"));
    return;
  }
  items.slice(0, 24).forEach((item) => {
    const node = template.content.firstElementChild.cloneNode(true);
    const title = $(".item-title", node);
    title.textContent = text(item.title, "未命名条目");
    title.href = item.url || `index.html?date=${encodeURIComponent(state.date)}`;
    $(".item-summary", node).textContent = cleanCopy(item.summary, 220);
    const reason = cleanCopy(item.pm_reason, 180);
    $(".item-reason", node).textContent = reason ? `推荐理由：${reason}` : "";
    $(".item-reason", node).hidden = !reason;
    const meta = $(".item-meta", node);
    meta.append(
      metaPill(item.pm_label || "未评分", item.pm_label === "必读" ? "must" : item.pm_label === "值得读" ? "worth" : ""),
      metaPill(scoreText(item.pm_score)),
      metaPill(item.source || item.author || "来源未知"),
    );
    mount.appendChild(node);
  });
}

function recommendedItems() {
  const fromDigest = state.digest?.high_value_items || [];
  const fromDay = state.day?.items || [];
  const byId = new Map();
  for (const item of [...fromDigest, ...fromDay]) {
    const key = item.item_id || item.url || item.title;
    if (!key || byId.has(key)) continue;
    byId.set(key, item);
  }
  let items = [...byId.values()].filter((item) => ["必读", "值得读"].includes(item.pm_label));
  if (!items.length) items = publicStories(fromDay, 24);
  if (state.filter === "must") items = items.filter((item) => item.pm_label === "必读");
  if (state.filter === "worth") items = items.filter((item) => item.pm_label === "值得读");
  if (!items.length && !fromDay.some((item) => item.pm_label)) items = publicStories(fromDay, 24);
  return items.sort((a, b) => Number(b.pm_score || 0) - Number(a.pm_score || 0));
}

function publicStories(items, limit = 5) {
  const candidates = [...items]
    .filter((item) => item.url && (item.pm_label === "必读" || item.pm_label === "值得读" || Number(item.pm_score || 0) >= 80))
    .sort((a, b) => Number(b.pm_score || 0) - Number(a.pm_score || 0));
  const fallback = candidates.length ? candidates : [...items].filter((item) => item.url && item.title);
  return fallback
    .slice(0, limit)
    .map((item) => ({
      title: item.title,
      url: item.url,
      summary: item.summary,
      pm_reason: item.pm_reason || `${item.source || item.author || "公开源"} · ${item.pm_label || "推荐"}`,
      related_entities: item.entity_tags || [],
      related_topics: item.topic_tags || [],
    }));
}

function publicDailyLine(items) {
  const top = publicStories(items).slice(0, 3).map((item) => item.title).filter(Boolean);
  if (!top.length) return "公开版数据已更新，先从今日高价值推荐开始读。";
  return `公开版数据已更新，今天先看：${top.join("、")}。`;
}

function firstStoryUrl(story) {
  const example = (story.examples || []).find((item) => item.url);
  return example?.url || null;
}

function shortDate(date) {
  const parsed = new Date(`${date}T12:00:00+08:00`);
  return new Intl.DateTimeFormat("zh-CN", {
    month: "numeric",
    day: "numeric",
    weekday: "short",
  }).format(parsed);
}

function syncUrlDate() {
  const url = new URL(window.location.href);
  url.searchParams.set("date", state.date);
  window.history.replaceState(null, "", url);
}

function metaPill(label, className = "") {
  const el = document.createElement("span");
  if (className) el.className = className;
  el.textContent = label;
  return el;
}

function scoreText(score) {
  if (score == null || score === "") return "无分数";
  return `${score} 分`;
}

function note(copy) {
  const el = document.createElement("div");
  el.className = "empty-note";
  el.textContent = copy;
  return el;
}

function renderError(error) {
  $("#main-judgment").textContent = "日报读取失败";
  $("#main-line").textContent = error?.message || "请稍后再试。";
  $("#story-list").innerHTML = "";
  $("#recommend-list").innerHTML = "";
  const el = document.createElement("div");
  el.className = "error-note";
  el.textContent = "检查 web/data/digest/index.json 是否已经生成并随 GitHub Pages 发布。";
  $("#story-list").appendChild(el);
}

function syncDateButtons() {
  const index = state.dates.indexOf(state.date);
  $("#next-date").disabled = index <= 0;
  $("#prev-date").disabled = index < 0 || index >= state.dates.length - 1;
}

boot();
