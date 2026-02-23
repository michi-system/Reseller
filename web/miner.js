const ISSUE_TARGET_OPTIONS = [
  ["model", "型番"],
  ["brand", "ブランド"],
  ["color", "色"],
  ["condition", "状態"],
  ["price", "価格"],
  ["accessory_source", "付属品"],
  ["bundle_market", "同梱差分"],
  ["shipping", "送料/配送"],
  ["other", "その他"],
];
const ISSUE_TARGET_LABEL_MAP = {
  ...Object.fromEntries(ISSUE_TARGET_OPTIONS),
  // 過去データ表示用（現在は選択肢として出さない）
  size: "サイズ",
  accessories: "付属品（旧）",
  fees: "手数料",
  fx: "為替",
};

const LIVE_FETCH_SOURCE_SITES = ["rakuten", "yahoo"];
const DEFAULT_MIN_MATCH_SCORE = 0.72;
const TAB_COUNT_CAP = 9999;

const STATUS_LABELS = {
  pending: "未レビュー",
  rejected: "否認済み",
  listed: "承認済み（ダミー出品）",
  approved: "自動承認（要最終確認）",
};

const SITE_LABELS = {
  ebay: "eBay",
  rakuten: "楽天市場",
  yahoo: "Yahoo!ショッピング",
  yahoo_shopping: "Yahoo!ショッピング",
  amazon: "Amazon",
  japan: "日本サイト",
  jp: "日本サイト",
};

const COLOR_MISSING_REASONS = new Set([
  "variant_color_missing_market",
  "color_missing_market",
  "model_code_variant_color_missing_market",
  "model_code_normalized_variant_color_missing_market",
  "model_code_color_missing_market",
  "model_code_normalized_color_missing_market",
  "jan_exact_variant_color_missing_market",
  "upc_exact_variant_color_missing_market",
  "ean_exact_variant_color_missing_market",
  "gtin_exact_variant_color_missing_market",
  "jan_exact_color_missing_market",
  "upc_exact_color_missing_market",
  "ean_exact_color_missing_market",
  "gtin_exact_color_missing_market",
]);

const BRAND_PATTERNS = [
  { label: "CASIO", tokens: ["CASIO", "カシオ"] },
  { label: "SEIKO", tokens: ["SEIKO", "セイコー"] },
  { label: "CITIZEN", tokens: ["CITIZEN", "シチズン"] },
  { label: "ORIENT", tokens: ["ORIENT", "オリエント"] },
  { label: "SONY", tokens: ["SONY", "ソニー"] },
  { label: "PANASONIC", tokens: ["PANASONIC", "パナソニック"] },
  { label: "NINTENDO", tokens: ["NINTENDO", "任天堂"] },
  { label: "APPLE", tokens: ["APPLE", "アップル"] },
  { label: "ANKER", tokens: ["ANKER"] },
  { label: "NIKE", tokens: ["NIKE", "ナイキ"] },
  { label: "ADIDAS", tokens: ["ADIDAS", "アディダス"] },
];

const COLOR_PATTERNS = [
  { label: "ブラック", tokens: ["BLACK", "BLK", "ブラック", "黒"] },
  { label: "ホワイト", tokens: ["WHITE", "WHT", "ホワイト", "白"] },
  { label: "シルバー", tokens: ["SILVER", "SLV", "シルバー", "銀"] },
  { label: "ゴールド", tokens: ["GOLD", "GLD", "ゴールド", "金"] },
  { label: "ブルー", tokens: ["BLUE", "BLU", "NAVY", "ブルー", "青", "紺"] },
  { label: "グリーン", tokens: ["GREEN", "GRN", "グリーン", "緑"] },
  { label: "レッド", tokens: ["RED", "レッド", "赤"] },
  { label: "グレー", tokens: ["GRAY", "GREY", "GRY", "グレー", "灰"] },
  { label: "ブラウン", tokens: ["BROWN", "BRN", "ブラウン", "茶"] },
  { label: "ベージュ", tokens: ["BEIGE", "ベージュ"] },
  { label: "ピンク", tokens: ["PINK", "ピンク"] },
  { label: "パープル", tokens: ["PURPLE", "パープル", "紫"] },
];

const MODEL_CODE_STOPWORDS = new Set([
  "WATCH", "JAPAN", "JAPANESE", "NEW", "UNUSED", "AUTHENTIC", "SHIPPING",
  "SOLAR", "ATOMIC", "DIGITAL", "ANALOG", "MODEL", "MENS", "LADIES", "WOMENS",
]);
const MODEL_PROMO_CONTEXT_MARKERS = [
  "OFF", "COUPON", "SALE", "POINT", "クーポン", "割引", "値引", "ポイント", "円", "%", "％", "倍",
];

const FALLBACK_CATEGORIES = [
  { value: "watch", label: "腕時計" },
  { value: "sneakers", label: "スニーカー" },
  { value: "streetwear", label: "ストリートウェア" },
  { value: "trading_cards", label: "トレーディングカード" },
  { value: "toys_collectibles", label: "ホビー・コレクティブル" },
  { value: "video_game_consoles", label: "ゲーム機本体" },
  { value: "audio", label: "オーディオ" },
];

const refs = {
  fetchQuery: document.getElementById("fetchQuery"),
  fetchBtn: document.getElementById("fetchBtn"),
  openSettingsBtn: document.getElementById("openSettingsBtn"),
  settingsOverlay: document.getElementById("settingsOverlay"),
  closeSettingsBtn: document.getElementById("closeSettingsBtn"),
  requireInStock: document.getElementById("requireInStock"),
  limitPerSite: document.getElementById("limitPerSite"),
  maxCandidates: document.getElementById("maxCandidates"),
  minMatchScore: document.getElementById("minMatchScore"),
  minProfitUsd: document.getElementById("minProfitUsd"),
  minMarginRate: document.getElementById("minMarginRate"),
  reloadBtn: document.getElementById("reloadBtn"),
  tabPending: document.getElementById("tabPending"),
  tabReviewed: document.getElementById("tabReviewed"),
  countPending: document.getElementById("countPending"),
  countReviewed: document.getElementById("countReviewed"),
  reviewList: document.getElementById("reviewList"),
  reviewListEmpty: document.getElementById("reviewListEmpty"),
  currentCandidateLabel: document.getElementById("currentCandidateLabel"),
  approveBtn: document.getElementById("approveBtn"),
  approveHint: document.getElementById("approveHint"),
  rejectBtn: document.getElementById("rejectBtn"),
  reasonText: document.getElementById("reasonText"),
  issueTargets: document.getElementById("issueTargets"),
  toast: document.getElementById("toast"),
  endpointLabel: document.getElementById("endpointLabel"),
  ebaySiteTag: document.getElementById("ebaySiteTag"),
  jpSiteTag: document.getElementById("jpSiteTag"),
  ebayImage: document.getElementById("ebayImage"),
  jpImage: document.getElementById("jpImage"),
  ebayTitle: document.getElementById("ebayTitle"),
  jpTitle: document.getElementById("jpTitle"),
  ebayExtracted: document.getElementById("ebayExtracted"),
  jpExtracted: document.getElementById("jpExtracted"),
  ebayPrice: document.getElementById("ebayPrice"),
  ebayShipping: document.getElementById("ebayShipping"),
  ebayTotal: document.getElementById("ebayTotal"),
  ebayExtraCosts: document.getElementById("ebayExtraCosts"),
  ebaySoldCount90d: document.getElementById("ebaySoldCount90d"),
  ebaySoldMin90d: document.getElementById("ebaySoldMin90d"),
  jpPrice: document.getElementById("jpPrice"),
  jpShipping: document.getElementById("jpShipping"),
  jpTotal: document.getElementById("jpTotal"),
  jpStockRow: document.getElementById("jpStockRow"),
  jpStockRule: document.getElementById("jpStockRule"),
  jpExtraInfo: document.getElementById("jpExtraInfo"),
  ebayLink: document.getElementById("ebayLink"),
  jpLink: document.getElementById("jpLink"),
  fxRate: document.getElementById("fxRate"),
  sumRevenue: document.getElementById("sumRevenue"),
  sumRevenueBreakdown: document.getElementById("sumRevenueBreakdown"),
  sumPurchase: document.getElementById("sumPurchase"),
  sumPurchaseBreakdown: document.getElementById("sumPurchaseBreakdown"),
  sumExpenses: document.getElementById("sumExpenses"),
  sumExpensesBreakdown: document.getElementById("sumExpensesBreakdown"),
  sumProfit: document.getElementById("sumProfit"),
  sumProfitBreakdown: document.getElementById("sumProfitBreakdown"),
  financeFormula: document.getElementById("financeFormula"),
  sumSoldCount90d: document.getElementById("sumSoldCount90d"),
  sumSoldMin90d: document.getElementById("sumSoldMin90d"),
  sumLiquidityGate: document.getElementById("sumLiquidityGate"),
  sumFeeRates: document.getElementById("sumFeeRates"),
  sumOtherCosts: document.getElementById("sumOtherCosts"),
  decisionTone: document.getElementById("decisionTone"),
  decisionLabel: document.getElementById("decisionLabel"),
  decisionSub: document.getElementById("decisionSub"),
  decisionReasons: document.getElementById("decisionReasons"),
  riskFlags: document.getElementById("riskFlags"),
  fetchStatusHeadline: document.getElementById("fetchStatusHeadline"),
  headerSeedStatus: document.getElementById("headerSeedStatus"),
  seedPoolSummary: document.getElementById("seedPoolSummary"),
  fetchStatsRows: document.getElementById("fetchStatsRows"),
  rpaProgressWrap: document.getElementById("rpaProgressWrap"),
  rpaProgressLabel: document.getElementById("rpaProgressLabel"),
  rpaProgressPercent: document.getElementById("rpaProgressPercent"),
  rpaProgressFill: document.getElementById("rpaProgressFill"),
  rpaProgressDetail: document.getElementById("rpaProgressDetail"),
  calcDigest: document.getElementById("calcDigest"),
  calcData: document.getElementById("calcData"),
  rawJson: document.getElementById("rawJson"),
};

const state = {
  queues: {
    pending: [],
    reviewed: [],
  },
  queueTotals: {
    pending: 0,
    reviewed: 0,
  },
  activeTab: "pending",
  current: null,
  lastFetch: null,
  detailCache: new Map(),
  detailFetchSeq: 0,
  scrollSelectRaf: null,
  selectingFromScroll: false,
  fetchInFlight: false,
  rpaProgressPollTimer: null,
  fetchStartedAtMs: 0,
  fetchStartedAtEpochSec: 0,
  fetchProgressRunId: "",
  fetchProgressSawRunning: false,
  optimisticProgress: 0,
  displayedProgress: 0,
  lastFetchProgressSource: "",
  seedPoolStatusSeq: 0,
  lastSeedPoolCategory: "",
  financeHeightRaf: null,
};

function openSettingsOverlay() {
  if (!refs.settingsOverlay) return;
  refs.settingsOverlay.hidden = false;
  document.body.style.overflow = "hidden";
}

function closeSettingsOverlay() {
  if (!refs.settingsOverlay) return;
  refs.settingsOverlay.hidden = true;
  document.body.style.overflow = "";
}

function setApproveHint(text) {
  if (!refs.approveHint) return;
  refs.approveHint.textContent = String(text || "");
}

function syncFinanceCellHeights() {
  const grid = document.querySelector(".finance-grid");
  if (!(grid instanceof HTMLElement)) return;
  const cells = Array.from(grid.querySelectorAll(":scope > .summary-cell"));
  if (cells.length === 0) return;

  for (const cell of cells) {
    if (!(cell instanceof HTMLElement)) continue;
    cell.style.minHeight = "";
  }

  let maxHeight = 0;
  for (const cell of cells) {
    if (!(cell instanceof HTMLElement)) continue;
    const h = Math.ceil(cell.getBoundingClientRect().height);
    if (h > maxHeight) maxHeight = h;
  }
  if (!Number.isFinite(maxHeight) || maxHeight <= 0) return;

  const target = maxHeight + 8;
  for (const cell of cells) {
    if (!(cell instanceof HTMLElement)) continue;
    cell.style.minHeight = `${target}px`;
  }
}

function scheduleFinanceCellHeightSync() {
  if (state.financeHeightRaf) return;
  state.financeHeightRaf = window.requestAnimationFrame(() => {
    state.financeHeightRaf = null;
    syncFinanceCellHeights();
  });
}

function resolveApiBase() {
  const params = new URLSearchParams(window.location.search || "");
  const fromQuery = (params.get("apiBase") || "").trim();
  if (fromQuery) {
    try {
      const url = new URL(fromQuery, window.location.origin);
      const normalized = `${url.protocol}//${url.host}`;
      window.localStorage.setItem("miner_api_base", normalized);
      return normalized;
    } catch (_) {
      // ignore invalid apiBase param
    }
  }

  const stored = (
    window.localStorage.getItem("miner_api_base")
    || ""
  ).trim();
  if (stored) {
    return stored.replace(/\/+$/, "");
  }

  const host = window.location.hostname;
  const isLocalHost = host === "127.0.0.1" || host === "localhost";
  if (window.location.protocol === "file:") {
    return "http://127.0.0.1:8012";
  }
  if (isLocalHost && window.location.port !== "8012") {
    return "http://127.0.0.1:8012";
  }
  if (window.location.port === "8012") {
    return "";
  }
  return "";
}

const API_BASE = resolveApiBase();

function showToast(message) {
  refs.toast.textContent = message;
  refs.toast.classList.add("show");
  window.setTimeout(() => refs.toast.classList.remove("show"), 2200);
}

function formatUsd(value) {
  if (!Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("ja-JP", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value);
}

function formatJpy(value) {
  if (!Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("ja-JP", {
    style: "currency",
    currency: "JPY",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value) {
  if (!Number.isFinite(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function toJpyFromUsd(usd, fxRate) {
  if (!Number.isFinite(usd) || !Number.isFinite(fxRate) || fxRate <= 0) return null;
  return usd * fxRate;
}

function toUsdFromJpy(jpy, fxRate) {
  if (!Number.isFinite(jpy) || !Number.isFinite(fxRate) || fxRate <= 0) return null;
  return jpy / fxRate;
}

function moneyPair({ jpy, usd, fxRate }) {
  const jpyNum = Number.isFinite(jpy) ? jpy : toJpyFromUsd(usd, fxRate);
  const usdNum = Number.isFinite(usd) ? usd : toUsdFromJpy(jpy, fxRate);
  return {
    jpy: Number.isFinite(jpyNum) ? jpyNum : null,
    usd: Number.isFinite(usdNum) ? usdNum : null,
  };
}

function moneyDualHtml({ jpy, usd, fxRate, align = "right", layout = "stack" }) {
  const pair = moneyPair({ jpy, usd, fxRate });
  if (!Number.isFinite(pair.jpy) && !Number.isFinite(pair.usd)) {
    return "-";
  }
  const alignClass = align === "left" ? "left" : "right";
  const hasJpy = Number.isFinite(pair.jpy);
  const hasUsd = Number.isFinite(pair.usd);
  const main = hasJpy ? formatJpy(pair.jpy) : "-";
  const sub = hasUsd ? formatUsd(pair.usd) : "-";
  if (layout === "inline") {
    if (hasJpy && hasUsd) {
      return `<span class="money-inline ${alignClass}"><span class="money-main">${escapeHtml(main)}</span><span class="money-sep">/</span><small class="money-sub">${escapeHtml(sub)}</small></span>`;
    }
    const single = hasJpy ? main : sub;
    return `<span class="money-inline ${alignClass}"><span class="money-main">${escapeHtml(single)}</span></span>`;
  }
  return `<span class="money-stack ${alignClass}"><span class="money-main">${escapeHtml(main)}</span><small class="money-sub">${escapeHtml(sub)}</small></span>`;
}

function setMoneyCell(el, { jpy, usd, fxRate, align = "right", layout = "stack" }) {
  if (!el) return;
  el.innerHTML = moneyDualHtml({ jpy, usd, fxRate, align, layout });
}

function moneyDualText({ jpy, usd, fxRate }) {
  const pair = moneyPair({ jpy, usd, fxRate });
  if (!Number.isFinite(pair.jpy) && !Number.isFinite(pair.usd)) {
    return "-";
  }
  const main = Number.isFinite(pair.jpy) ? formatJpy(pair.jpy) : "-";
  const sub = Number.isFinite(pair.usd) ? formatUsd(pair.usd) : "-";
  return `${main} / ${sub}`;
}

function moneyDualInlineHtml({ jpy, usd, fxRate, align = "left" }) {
  return moneyDualHtml({ jpy, usd, fxRate, align, layout: "inline" });
}

function shippingText({ jpy, usd, fxRate }) {
  if ((Number.isFinite(usd) && usd === 0) || (Number.isFinite(jpy) && jpy === 0)) {
    return "送料無料";
  }
  return moneyDualText({ jpy, usd, fxRate });
}

function shippingInlineHtml({ jpy, usd, fxRate, align = "left" }) {
  if ((Number.isFinite(usd) && usd === 0) || (Number.isFinite(jpy) && jpy === 0)) {
    return "送料無料";
  }
  return moneyDualInlineHtml({ jpy, usd, fxRate, align });
}

function setShippingCell(el, { jpy, usd, fxRate }) {
  if (!el) return;
  if ((Number.isFinite(usd) && usd === 0) || (Number.isFinite(jpy) && jpy === 0)) {
    el.textContent = "送料無料";
    return;
  }
  setMoneyCell(el, { jpy, usd, fxRate });
}

function setOptionalNote(el, text, fallback = "") {
  if (!el) return;
  const value = String(text || "").trim();
  if (value) {
    el.textContent = value;
    el.style.display = "block";
    return;
  }
  if (fallback) {
    el.textContent = String(fallback);
    el.style.display = "block";
    return;
  }
  el.textContent = "";
  el.style.display = "none";
}

function setOptionalNoteHtml(el, html, fallback = "") {
  if (!el) return;
  const value = String(html || "").trim();
  if (value) {
    el.innerHTML = value;
    el.style.display = "block";
    return;
  }
  if (fallback) {
    el.textContent = String(fallback);
    el.style.display = "block";
    return;
  }
  el.textContent = "";
  el.style.display = "none";
}

function summaryLinesHtml(lines) {
  if (!Array.isArray(lines)) return "";
  return lines
    .map((line) => String(line || "").trim())
    .filter(Boolean)
    .map((line) => {
      // 既に summary-line を含むHTMLは二重ラップしない（CSS競合回避）
      if (line.includes("summary-line")) return line;
      const compact = line.includes("money-inline") ? " summary-line-compact" : "";
      return `<span class="summary-line${compact}">${line}</span>`;
    })
    .join("");
}

function summaryMoneyLineHtml(label, { jpy, usd, fxRate, align = "left" }) {
  return `<span class="summary-line summary-line-money"><span class="summary-line-label">${escapeHtml(label)}</span>${moneyDualInlineHtml({ jpy, usd, fxRate, align })}</span>`;
}

function normalizeIdentifierMap(value) {
  if (!value || typeof value !== "object") return {};
  const out = {};
  for (const [k, v] of Object.entries(value)) {
    const key = String(k || "").trim().toLowerCase();
    const text = String(v || "").trim();
    if (!key || !text) continue;
    out[key] = text;
  }
  return out;
}

function conditionToJa(raw) {
  const text = String(raw || "").trim();
  if (!text) return "-";
  const upper = text.toUpperCase();
  if (upper.includes("NEW") || text.includes("新品")) return `新品 (${text})`;
  if (upper.includes("USED") || text.includes("中古")) return `中古 (${text})`;
  return text;
}

function extractBrandFromTitle(title) {
  const upper = String(title || "").normalize("NFKC").toUpperCase();
  for (const row of BRAND_PATTERNS) {
    for (const token of row.tokens) {
      if (upper.includes(String(token).toUpperCase())) return row.label;
    }
  }
  return "";
}

function extractColorFromTitle(title) {
  const upper = String(title || "").normalize("NFKC").toUpperCase();
  for (const row of COLOR_PATTERNS) {
    for (const token of row.tokens) {
      if (upper.includes(String(token).toUpperCase())) return row.label;
    }
  }
  return "";
}

function extractModelCodesFromTitle(title) {
  const upper = String(title || "").normalize("NFKC").toUpperCase();
  const re = /[A-Z0-9][A-Z0-9-]{3,}/g;
  const out = [];
  const seen = new Set();
  for (const m of upper.matchAll(re)) {
    const token = String(m[0] || "");
    const norm = token.replace(/^-+|-+$/g, "");
    if (!norm) continue;
    if (MODEL_CODE_STOPWORDS.has(norm)) continue;
    if (norm.length < 4) continue;
    if (!/[A-Z]/.test(norm)) continue;
    if (!/[0-9]/.test(norm)) continue;
    const start = Number.isFinite(m.index) ? Number(m.index) : upper.indexOf(token);
    const safeStart = Number.isFinite(start) && start >= 0 ? start : 0;
    const context = upper.slice(Math.max(0, safeStart - 10), Math.min(upper.length, safeStart + norm.length + 10));
    if (MODEL_PROMO_CONTEXT_MARKERS.some((marker) => context.includes(marker))) continue;
    if (seen.has(norm)) continue;
    seen.add(norm);
    out.push(norm);
  }
  return out.slice(0, 6);
}

function compactList(values) {
  const out = [];
  const seen = new Set();
  for (const raw of values) {
    const v = String(raw || "").trim();
    if (!v) continue;
    const key = v.toUpperCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out;
}

function buildExtractedSnapshot(side) {
  const ids = normalizeIdentifierMap(side.identifiers);
  const brand = ids.brand || ids.manufacturer || ids.maker || extractBrandFromTitle(side.title);
  const model = ids.model || ids.model_number || ids.item_model_number || ids.mpn || "";
  const modelCandidates = compactList([model, ...extractModelCodesFromTitle(side.title)]);
  const color = ids.color || ids.colour || extractColorFromTitle(side.title);
  const condition = conditionToJa(side.condition);
  const barcodeParts = [];
  if (ids.jan) barcodeParts.push(`JAN:${ids.jan}`);
  if (ids.upc) barcodeParts.push(`UPC:${ids.upc}`);
  if (ids.ean) barcodeParts.push(`EAN:${ids.ean}`);
  if (ids.gtin) barcodeParts.push(`GTIN:${ids.gtin}`);
  const others = [];
  if (modelCandidates.length > 1) others.push(`候補:${modelCandidates.slice(1, 4).join(", ")}`);
  if (barcodeParts.length) others.push(barcodeParts.join(" / "));
  if (side.itemId) others.push(`ID:${side.itemId}`);
  return {
    brand: brand || "-",
    model: modelCandidates[0] || "-",
    color: color || "-",
    condition: condition || "-",
    other: others.join(" | ") || "-",
  };
}

function buildExtractedFields(snapshot) {
  return [
    { key: "メーカー", val: snapshot.brand || "-" },
    { key: "型番", val: snapshot.model || "-" },
    { key: "色", val: snapshot.color || "-" },
    { key: "状態", val: snapshot.condition || "-" },
    { key: "その他", val: snapshot.other || "-" },
  ];
}

function renderExtracted(el, rows) {
  if (!el) return;
  el.innerHTML = rows.map((row) => {
    const key = escapeHtml(String(row?.key || "-"));
    const val = escapeHtml(String(row?.val || "-"));
    return `<div class="extract-row"><span class="k">${key}</span><strong class="v">${val}</strong></div>`;
  }).join("");
}

function isMissingExtractedValue(value) {
  const text = String(value || "").trim();
  return !text || text === "-" || text === "未取得";
}

function updateIssueTargetHighlights(pair) {
  const source = (pair && typeof pair.source === "object" && pair.source) ? pair.source : {};
  const market = (pair && typeof pair.market === "object" && pair.market) ? pair.market : {};
  const missing = {
    brand: isMissingExtractedValue(source.brand) || isMissingExtractedValue(market.brand),
    model: isMissingExtractedValue(source.model) || isMissingExtractedValue(market.model),
    color: isMissingExtractedValue(source.color) || isMissingExtractedValue(market.color),
    condition: isMissingExtractedValue(source.condition) || isMissingExtractedValue(market.condition),
    other: isMissingExtractedValue(source.other) || isMissingExtractedValue(market.other),
  };

  refs.issueTargets.querySelectorAll("label[data-issue-key]").forEach((label) => {
    const key = String(label.getAttribute("data-issue-key") || "").trim();
    const flagged = Boolean(missing[key]);
    label.classList.toggle("auto-missing", flagged);
    if (flagged) {
      label.setAttribute("title", "抽出情報が不足しています");
    } else {
      label.removeAttribute("title");
    }
  });
}

function labelForStatus(status) {
  const key = String(status || "").toLowerCase();
  return STATUS_LABELS[key] || key || "-";
}

function labelForSite(site) {
  const key = String(site || "").toLowerCase();
  return SITE_LABELS[key] || site || "-";
}

function formatIsoShort(text) {
  const raw = String(text || "").trim();
  if (!raw) return "-";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function getLatestRejection(candidate) {
  const rows = Array.isArray(candidate?.rejections) ? candidate.rejections : [];
  return rows.length > 0 ? rows[0] : null;
}

function formatIssueTargetsJa(targets) {
  if (!Array.isArray(targets) || targets.length === 0) return "-";
  const labels = targets.map((t) => {
    const key = String(t || "").trim();
    return ISSUE_TARGET_LABEL_MAP[key] || key || "-";
  }).filter(Boolean);
  return labels.length > 0 ? labels.join(" / ") : "-";
}

function compactText(text, maxLen = 54) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  if (raw.length <= maxLen) return raw;
  return `${raw.slice(0, maxLen - 1)}…`;
}

function stopReasonLabel(reason) {
  const key = String(reason || "").trim().toLowerCase();
  if (!key) return "-";
  const map = {
    query_exhausted: "探索範囲を完走",
    target_reached: "目標件数に到達",
    max_calls_reached: "API呼び出し上限に到達",
    low_yield_stop: "増分が少ないため停止",
    skipped_no_market_hits: "eBayヒット0でスキップ",
    rpa_daily_limit_reached: "Product Research上限で停止",
    seed_batch_completed: "Seedバッチを完了",
    seed_pool_empty: "有効なSeedなし",
    timebox_reached: "時間上限で停止",
    error: "APIエラーで停止",
  };
  return map[key] || key;
}

function stopReasonDetail(info) {
  const reason = String(info?.stop_reason || "").trim().toLowerCase();
  const calls = Number(info?.calls_made || 0);
  const maxCalls = Number(info?.max_calls || 0);
  if (reason === "max_calls_reached") {
    return maxCalls > 0
      ? `この探索で設定した上限 ${maxCalls} 回に到達したため停止（実行 ${calls} 回）`
      : "この探索で設定したAPI呼び出し上限に到達したため停止";
  }
  if (reason === "target_reached") return "目標件数を満たしたため早期終了";
  if (reason === "query_exhausted") return "設定した検索語とページ範囲を最後まで探索";
  if (reason === "low_yield_stop") return "新規取得が少ない状態が続いたため早期停止";
  if (reason === "skipped_no_market_hits") return "eBayヒット0のため日本側取得をスキップ";
  if (reason === "rpa_daily_limit_reached") return "Product Researchの1日上限に到達したため停止";
  if (reason === "error") return "APIエラー発生のため停止";
  return "";
}

function skipReasonLabel(key) {
  const k = String(key || "").trim().toLowerCase();
  const map = {
    skipped_low_match: "一致不足",
    skipped_unprofitable: "低利益",
    skipped_low_margin: "低粗利率",
    skipped_low_liquidity: "低流動性",
    skipped_liquidity_unavailable: "流動性未取得",
    skipped_missing_sold_min: "90日最低未取得",
    skipped_missing_sold_sample: "売却サンプル欠損",
    skipped_below_sold_min: "仕入>=90日最低",
    skipped_implausible_sold_min: "90日最低異常値",
    skipped_ambiguous_model_title: "曖昧型番タイトル",
    skipped_blocked: "否認ブロック",
  };
  return map[k] || k || "-";
}

function isRpaDailyLimitReached(payload) {
  if (!payload || typeof payload !== "object") return false;
  if (Boolean(payload.rpa_daily_limit_reached)) return true;
  const refresh = (payload.liquidity_rpa_refresh && typeof payload.liquidity_rpa_refresh === "object")
    ? payload.liquidity_rpa_refresh
    : null;
  const reason = String(refresh?.reason || "").trim().toLowerCase();
  if (reason === "daily_limit_reached") return true;
  if (Boolean(refresh?.daily_limit_reached)) return true;
  const timedReason = String(payload?.timed_fetch?.stop_reason || "").trim().toLowerCase();
  if (timedReason === "rpa_daily_limit_reached") return true;
  return false;
}

function refillReasonLabel(reason) {
  const key = String(reason || "").trim().toLowerCase();
  if (!key) return "不明";
  const map = {
    snapshot: "現在のSeedプール状態",
    threshold_not_reached: "補充不要（しきい値以上）",
    bootstrap_refilled: "カテゴリ知識で補充",
    refilled: "補充実行",
    target_reached: "目標補充件数に到達",
    soft_target_reached: "80%到達で補充停止",
    fresh_window_skip: "7日以内ページのみのため補充待機",
    zero_history_top_rechecked: "空ページ履歴のため先頭を再確認",
    category_cooldown: "カテゴリ補充クールダウン中",
    rank_limit_cooldown: "深掘り上限でクールダウン",
    empty_result_cooldown: "検索結果空で一時停止",
    daily_limit_reached: "Product Research上限到達",
    empty_result_page: "補充ページ0件（既存Seedで探索継続）",
  };
  return map[key] || key;
}

function getSeedPoolView(payload) {
  const seedPool = (payload && typeof payload === "object" && payload.seed_pool && typeof payload.seed_pool === "object")
    ? payload.seed_pool
    : {};
  const refill = (seedPool.refill && typeof seedPool.refill === "object") ? seedPool.refill : {};
  const categoryLabel = String(seedPool.category_label || seedPool.category_key || "カテゴリ").trim();
  const availableAfter = Number(seedPool.available_after_refill || 0);
  const selectedCount = Number(seedPool.selected_seed_count || 0);
  const seedCountRaw = Number(seedPool.seed_count ?? availableAfter ?? selectedCount ?? 0);
  const seedCount = Number.isFinite(seedCountRaw) ? seedCountRaw : 0;
  const skippedLowQuality = Number(seedPool.skipped_low_quality_count || 0);
  const reason = refillReasonLabel(refill.reason);
  const lastRefillAt = String(refill.last_refill_at || "").trim();
  const cooldownUntil = String(refill.cooldown_until || "").trim();
  const dailyLimitReached = Boolean(payload?.rpa_daily_limit_reached) || Boolean(refill.daily_limit_reached);
  const addedCount = Number(refill.added_count || 0);
  const pageRuns = Array.isArray(refill.page_runs) ? refill.page_runs.length : 0;
  return {
    categoryLabel,
    availableAfter,
    selectedCount,
    seedCount,
    skippedLowQuality,
    reason,
    lastRefillAt,
    cooldownUntil,
    dailyLimitReached,
    addedCount,
    pageRuns,
  };
}

function renderHeaderSeedStatus(payload, { loading = false, failed = false } = {}) {
  if (!refs.headerSeedStatus) return;
  const category = String(refs.fetchQuery?.value || "").trim() || "-";
  if (loading) {
    refs.headerSeedStatus.classList.remove("warn");
    refs.headerSeedStatus.innerHTML = `
      <span class="header-seed-chip">カテゴリ: ${escapeHtml(category)}</span>
      <span class="header-seed-chip">Seed情報を取得中</span>
    `;
    return;
  }
  if (failed) {
    refs.headerSeedStatus.classList.add("warn");
    refs.headerSeedStatus.innerHTML = `
      <span class="header-seed-chip">カテゴリ: ${escapeHtml(category)}</span>
      <span class="header-seed-chip">Seed情報の取得に失敗</span>
    `;
    return;
  }
  if (!payload || typeof payload !== "object") {
    refs.headerSeedStatus.classList.remove("warn");
    refs.headerSeedStatus.innerHTML = `
      <span class="header-seed-chip">カテゴリ: ${escapeHtml(category)}</span>
      <span class="header-seed-chip">Seed数: -</span>
      <span class="header-seed-chip">補充状態: -</span>
      <span class="header-seed-chip">更新: -</span>
    `;
    return;
  }
  const view = getSeedPoolView(payload);
  refs.headerSeedStatus.classList.toggle("warn", Boolean(view.dailyLimitReached));
  refs.headerSeedStatus.innerHTML = `
    <span class="header-seed-chip">カテゴリ: ${escapeHtml(view.categoryLabel || category)}</span>
    <span class="header-seed-chip">Seed数: ${Number.isFinite(view.seedCount) ? view.seedCount : 0}件</span>
    <span class="header-seed-chip">補充状態: ${escapeHtml(view.reason)}</span>
    <span class="header-seed-chip">更新: ${escapeHtml(view.lastRefillAt ? formatIsoShort(view.lastRefillAt) : "未補充")}</span>
  `;
}

function renderSeedPoolSummary(payload) {
  if (!refs.seedPoolSummary) return;
  if (!payload || typeof payload !== "object") {
    refs.seedPoolSummary.classList.remove("warn");
    refs.seedPoolSummary.textContent = "Seedプール情報を取得できませんでした。";
    return;
  }
  const seedPool = (payload.seed_pool && typeof payload.seed_pool === "object")
    ? payload.seed_pool
    : null;
  if (!seedPool) {
    refs.seedPoolSummary.classList.remove("warn");
    refs.seedPoolSummary.textContent = "今回のレスポンスにSeedプール情報は含まれていません。";
    return;
  }
  const refill = (seedPool.refill && typeof seedPool.refill === "object") ? seedPool.refill : {};
  const view = getSeedPoolView(payload);
  const cooldownUntil = view.cooldownUntil;
  const lastRefillAt = view.lastRefillAt;
  const lastRefillMessage = String(refill.last_refill_message || "").trim();
  const categoryLabel = view.categoryLabel;
  const seedCount = view.seedCount;
  const skippedLowQuality = view.skippedLowQuality;
  const addedCount = Number(refill.added_count || 0);
  const beforeCount = Number(refill.available_before || 0);
  const afterCount = Number(refill.available_after || 0);
  const pageRuns = view.pageRuns;
  const skippedFreshPages = Number(refill.skipped_fresh_pages || 0);
  const reason = view.reason;

  refs.seedPoolSummary.classList.toggle("warn", view.dailyLimitReached);
  refs.seedPoolSummary.innerHTML = `
    <div><strong>Seedプール: ${escapeHtml(categoryLabel)}</strong> / ${escapeHtml(reason)}</div>
    <div class="seed-pool-chip-row">
      <span class="seed-pool-chip">Seed数 ${Number.isFinite(seedCount) ? seedCount : 0}件</span>
      <span class="seed-pool-chip">補充 +${Number.isFinite(addedCount) ? addedCount : 0}件</span>
      ${pageRuns > 0 ? `<span class="seed-pool-chip">補充ページ ${pageRuns}ページ</span>` : ""}
      ${skippedFreshPages > 0 ? `<span class="seed-pool-chip">直近取得スキップ ${skippedFreshPages}ページ</span>` : ""}
      ${skippedLowQuality > 0 ? `<span class="seed-pool-chip">低品質除外 ${skippedLowQuality}件</span>` : ""}
    </div>
    <ul class="compact-list">
      <li>補充前: ${Number.isFinite(beforeCount) ? beforeCount : 0}件 / 補充後: ${Number.isFinite(afterCount) ? afterCount : 0}件</li>
      ${lastRefillAt ? `<li>最終補充時刻: ${escapeHtml(formatIsoShort(lastRefillAt))}</li>` : ""}
      ${lastRefillMessage ? `<li>補充ログ: ${escapeHtml(lastRefillMessage)}</li>` : ""}
    </ul>
    ${cooldownUntil ? `<div class="fetch-note">再補充可能時刻: ${escapeHtml(formatIsoShort(cooldownUntil))}</div>` : ""}
  `;
}

function saleBasisLabel(basis) {
  const key = String(basis || "").trim().toLowerCase();
  if (!key) return "-";
  const map = {
    sold_price_min_90d: "90日最低成約を採用",
    sold_price_median_90d: "90日中央値を採用",
    active_listing_price: "現行出品価格を採用",
  };
  return map[key] || key;
}

function setDecisionSummary({ label = "-", sub = "候補を選択してください", tone = "info" } = {}) {
  if (refs.decisionLabel) refs.decisionLabel.textContent = label;
  if (refs.decisionSub) refs.decisionSub.textContent = sub;
  if (!refs.decisionTone) return;
  refs.decisionTone.classList.remove("good", "warn", "bad", "info");
  refs.decisionTone.classList.add(tone);
}

function buildDecisionSummary(candidate, normalized) {
  const status = String(candidate?.status || "").toLowerCase();
  const profitUsd = toNumber(normalized?.expectedProfitUsd);
  const margin = toNumber(normalized?.expectedMarginRate);
  const soldCount = toNumber(normalized?.ebay?.soldCount90d);
  const hasSoldCount = Number.isFinite(soldCount) && soldCount >= 0;
  const hasLiquidity = hasSoldCount && soldCount > 0;
  const hasProfit = Number.isFinite(profitUsd) && profitUsd > 0;
  const hasMargin = Number.isFinite(margin) && margin >= 0.03;

  if (status === "listed") {
    return {
      label: "承認済み（ダミー出品）",
      sub: "最終レビュー済みの候補です。",
      tone: "info",
    };
  }
  if (status === "rejected") {
    return {
      label: "否認済み",
      sub: "否認理由を確認してロジック改善に反映してください。",
      tone: "bad",
    };
  }
  if (status === "approved") {
    if (hasProfit && hasMargin && hasLiquidity) {
      return {
        label: "自動承認（最終確認待ち）",
        sub: "利益・粗利率・90日売却件数を満たしています。",
        tone: "good",
      };
    }
    return {
      label: "自動承認（要確認）",
      sub: "自動判定は通過済み。流動性か価格条件を目視で再確認してください。",
      tone: "warn",
    };
  }
  if (hasProfit && hasMargin && hasLiquidity) {
    return {
      label: "承認候補",
      sub: "利益・粗利率・90日売却件数を満たしています。",
      tone: "good",
    };
  }
  if (!hasProfit || !hasMargin) {
    return {
      label: "否認推奨",
      sub: "利益または粗利率が基準未達です。",
      tone: "bad",
    };
  }
  return {
    label: "要確認",
    sub: hasSoldCount
      ? "90日売却件数が不足しています。"
      : "90日売却データ未取得のため要確認です。",
    tone: "warn",
  };
}

function buildDecisionReasons(candidate, normalized, colorRisk) {
  const reasons = [];
  const profitUsd = toNumber(normalized?.expectedProfitUsd);
  const marginRate = toNumber(normalized?.expectedMarginRate);
  const soldCount = toNumber(normalized?.ebay?.soldCount90d);
  const gatePassed = Boolean(normalized?.liquidity?.gate_passed);
  const gateReason = String(normalized?.liquidity?.gate_reason || "").trim();

  if (Number.isFinite(profitUsd)) {
    if (profitUsd > 0) {
      reasons.push({
        html: `最終利益 ${moneyDualInlineHtml({ jpy: normalized?.expectedProfitJpy, usd: profitUsd, fxRate: normalized?.fxRate })}`,
        tone: "good",
      });
    } else {
      reasons.push({
        html: `最終利益が赤字 ${moneyDualInlineHtml({ jpy: normalized?.expectedProfitJpy, usd: profitUsd, fxRate: normalized?.fxRate })}`,
        tone: "warn",
      });
    }
  } else {
    reasons.push({ text: "最終利益データ未取得", tone: "warn" });
  }

  if (Number.isFinite(marginRate)) {
    if (marginRate >= 0.03) reasons.push({ text: `粗利率 ${formatPercent(marginRate)}（基準以上）`, tone: "good" });
    else reasons.push({ text: `粗利率 ${formatPercent(marginRate)}（基準未達）`, tone: "warn" });
  } else {
    reasons.push({ text: "粗利率データ未取得", tone: "warn" });
  }

  if (Number.isFinite(soldCount)) {
    reasons.push({ text: `90日売却件数 ${soldCount}件`, tone: soldCount > 0 ? "good" : "warn" });
  } else {
    reasons.push({ text: "90日売却件数 未取得", tone: "warn" });
  }

  if (gatePassed) reasons.push({ text: "流動性ゲート 通過", tone: "good" });
  else reasons.push({ text: `流動性ゲート 除外${gateReason ? ` (${gateReason})` : ""}`, tone: "warn" });

  if (colorRisk?.hasColorMissingRisk) {
    reasons.push({ text: "色情報不足のため目視確認が必要", tone: "warn" });
  }

  const status = String(candidate?.status || "").toLowerCase();
  if (status === "listed") reasons.push({ text: "ダミー出品済み", tone: "good" });
  if (status === "rejected") reasons.push({ text: "過去に否認済み", tone: "warn" });

  return reasons.slice(0, 5);
}

function boundedNumber(inputEl, fallback, min, max) {
  const raw = Number(inputEl?.value);
  if (!Number.isFinite(raw)) return fallback;
  return Math.min(max, Math.max(min, raw));
}

function buildFetchConfig() {
  return {
    requireInStock: refs.requireInStock ? Boolean(refs.requireInStock.checked) : true,
    limitPerSite: Math.round(boundedNumber(refs.limitPerSite, 20, 1, 30)),
    maxCandidates: Math.round(boundedNumber(refs.maxCandidates, 20, 1, 50)),
    minMatchScore: boundedNumber(refs.minMatchScore, DEFAULT_MIN_MATCH_SCORE, 0.5, 0.99),
    minProfitUsd: boundedNumber(refs.minProfitUsd, 0.01, 0.0, 999999),
    minMarginRate: boundedNumber(refs.minMarginRate, 0.03, 0.0, 1.0),
  };
}

function toNumber(v) {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (typeof v === "string" && v.trim()) {
    const p = Number(v);
    return Number.isFinite(p) ? p : null;
  }
  return null;
}

function isColorMissingReason(reason) {
  const key = String(reason || "").trim().toLowerCase();
  if (!key) return false;
  if (COLOR_MISSING_REASONS.has(key)) return true;
  return key.includes("color_missing_market");
}

function getColorRiskInfo(candidate) {
  const meta = (candidate && typeof candidate.metadata === "object" && candidate.metadata) ? candidate.metadata : {};
  const autoReview = (meta && typeof meta.auto_miner === "object" && meta.auto_miner) ? meta.auto_miner : null;
  const autoMetrics = (autoReview && typeof autoReview.metrics === "object" && autoReview.metrics) ? autoReview.metrics : {};
  const fetchReason = String(meta.match_reason || "").trim();
  const rematchReason = String(autoMetrics.rematch_reason || "").trim();
  const reasons = [];
  if (isColorMissingReason(fetchReason)) reasons.push(fetchReason);
  if (isColorMissingReason(rematchReason)) reasons.push(rematchReason);
  return {
    hasColorMissingRisk: reasons.length > 0,
    reasons: Array.from(new Set(reasons)),
    fetchReason,
    rematchReason,
  };
}

function getSoldMinOutlierInfo(liquidity) {
  const liq = (liquidity && typeof liquidity === "object") ? liquidity : null;
  if (!liq) return { isOutlier: false, soldMin: null, soldMinRaw: null, soldMedian: null, ratio: null };
  const meta = (typeof liq.metadata === "object" && liq.metadata) ? liq.metadata : {};
  const soldMin = toNumber(meta.sold_price_min);
  const soldMinRaw = toNumber(meta.sold_price_min_raw ?? meta.sold_price_min);
  const soldMedian = toNumber(liq.sold_price_median);
  const ratioFromMeta = toNumber(meta.sold_price_min_ratio_vs_median);
  const heuristicRatio = (Number.isFinite(soldMinRaw) && Number.isFinite(soldMedian) && soldMedian > 0)
    ? soldMinRaw / soldMedian
    : null;
  const ratio = Number.isFinite(ratioFromMeta) ? ratioFromMeta : heuristicRatio;
  const threshold = 0.35;
  const outlierFlag = Boolean(meta.sold_price_min_outlier);
  const heuristicOutlier = Number.isFinite(soldMinRaw) && Number.isFinite(ratio) && ratio < threshold;
  return {
    isOutlier: outlierFlag || heuristicOutlier,
    soldMin: Number.isFinite(soldMin) ? soldMin : null,
    soldMinRaw: Number.isFinite(soldMinRaw) ? soldMinRaw : null,
    soldMedian: Number.isFinite(soldMedian) ? soldMedian : null,
    ratio: Number.isFinite(ratio) ? ratio : null,
  };
}

function pick(meta, keys, fallback = null) {
  if (!meta || typeof meta !== "object") return fallback;
  for (const k of keys) {
    if (meta[k] !== undefined && meta[k] !== null && `${meta[k]}`.trim() !== "") {
      return meta[k];
    }
  }
  return fallback;
}

function pickNumber(meta, keys, fallback = null) {
  const v = pick(meta, keys, fallback);
  return toNumber(v);
}

function firstOfArray(meta, keys) {
  for (const k of keys) {
    const arr = meta[k];
    if (Array.isArray(arr) && arr.length > 0) {
      if (typeof arr[0] === "string") return arr[0];
      if (arr[0] && typeof arr[0].imageUrl === "string") return arr[0].imageUrl;
    }
  }
  return null;
}

function pickImage(meta, side) {
  if (side === "ebay") {
    return pick(meta, ["ebay_image_url", "market_image_url", "market_image"])
      || firstOfArray(meta, ["ebay_image_urls", "market_image_urls"]);
  }
  return pick(meta, ["jp_image_url", "source_image_url", "source_image"])
    || firstOfArray(meta, ["jp_image_urls", "source_image_urls"]);
}

function normalize(candidate) {
  const meta = candidate.metadata || {};
  const fxRate = toNumber(candidate.fx_rate) || pickNumber(meta, ["fx_rate"], null) || 0;

  const soldItemTitle = String(pick(meta, ["ebay_sold_title"], "") || "").trim();
  const soldItemUrl = pick(meta, ["ebay_sold_item_url"], null);
  const soldImageUrl = pick(meta, ["ebay_sold_image_url"], null);
  const soldItemPrice = pickNumber(meta, ["ebay_sold_price_usd"], null);
  const hasSoldItemReference = Boolean(soldItemTitle || soldItemUrl || soldImageUrl || Number.isFinite(soldItemPrice));

  const ebayPrice = pickNumber(
    meta,
    ["market_price_basis_usd", "ebay_sold_price_usd", "ebay_price_usd", "market_price_usd", "sale_price_usd"],
    null
  );
  const ebayShipping = pickNumber(meta, ["market_shipping_basis_usd", "ebay_shipping_usd", "market_shipping_usd"], 0);
  const jpPrice = pickNumber(meta, ["source_price_basis_jpy", "jp_price_jpy", "source_price_jpy", "purchase_price_jpy"], null);
  const jpShipping = pickNumber(meta, ["source_shipping_basis_jpy", "jp_shipping_jpy", "source_shipping_jpy", "domestic_shipping_jpy"], 0);

  const ebayTotal = (ebayPrice ?? 0) + (ebayShipping ?? 0);
  const jpTotalJpy = (jpPrice ?? 0) + (jpShipping ?? 0);
  const jpTotalUsd = fxRate > 0 ? jpTotalJpy / fxRate : null;
  const expectedProfitUsd = toNumber(candidate.expected_profit_usd);
  const expectedProfitJpy = Number.isFinite(expectedProfitUsd) && fxRate > 0
    ? expectedProfitUsd * fxRate
    : null;
  const sourceIdentifiers = normalizeIdentifierMap(meta.source_identifiers);
  const marketIdentifiers = normalizeIdentifierMap(meta.market_identifiers);
  const liquidity = (meta && typeof meta.liquidity === "object") ? meta.liquidity : null;
  const liqMeta = (liquidity && typeof liquidity.metadata === "object") ? liquidity.metadata : {};
  const soldCount90d = toNumber(liquidity?.sold_90d_count);
  const soldMin90d = toNumber(liqMeta?.sold_price_min ?? liqMeta?.sold_price_min_raw);
  const saleBasisType = String(pick(meta, ["market_price_basis_type"], "active_listing_price") || "active_listing_price");
  const soldBasisRequiresSoldUrl = saleBasisType === "sold_price_min_90d";
  const soldItemUrlText = String(soldItemUrl || "").trim();
  const soldReferenceLinkAvailable = soldItemUrlText.length > 0;
  const sourceStockStatusRaw = pick(meta, ["source_stock_status"], "");
  const sourceStockStatus = String(sourceStockStatusRaw || "").trim();
  const calcInput = (meta && typeof meta.calc_input === "object") ? meta.calc_input : {};
  const calcBreakdown = (meta && typeof meta.calc_breakdown === "object") ? meta.calc_breakdown : {};
  const revenueUsd = pickNumber(meta, ["market_revenue_basis_usd"], toNumber(calcBreakdown.revenue_usd));
  const jpCostUsd = toNumber(calcBreakdown.jpy_cost_total_usd);
  const variableFeeUsd = toNumber(calcBreakdown.variable_fee_usd);
  const usdCostTotal = toNumber(calcBreakdown.usd_cost_total);
  const intlShippingUsd = toNumber(calcInput.international_shipping_usd);
  const customsUsd = toNumber(calcInput.customs_usd);
  const packagingUsd = toNumber(calcInput.packaging_usd);
  const fixedFeeUsd = toNumber(calcInput.fixed_fee_usd);
  const miscCostUsd = toNumber(calcInput.misc_cost_usd);
  const expenseFromTotal = (Number.isFinite(usdCostTotal) && Number.isFinite(jpCostUsd))
    ? Math.max(0, usdCostTotal - jpCostUsd)
    : null;
  const expenseFromParts = [variableFeeUsd, intlShippingUsd, customsUsd, packagingUsd, fixedFeeUsd, miscCostUsd]
    .filter((v) => Number.isFinite(v))
    .reduce((acc, v) => acc + v, 0);
  const expensesUsd = Number.isFinite(expenseFromTotal) ? expenseFromTotal : (expenseFromParts > 0 ? expenseFromParts : null);
  const grossDiffUsd = (Number.isFinite(revenueUsd) && Number.isFinite(jpCostUsd))
    ? revenueUsd - jpCostUsd
    : (Number.isFinite(ebayTotal) && Number.isFinite(jpTotalUsd) ? ebayTotal - jpTotalUsd : null);
  const ebayItemIdRaw = String(candidate.market_item_id || "").trim();
  const ebayActiveItemIdRaw = String(pick(meta, ["market_item_id_active"], "") || "").trim();
  const ebayItemId = ebayItemIdFromAny(ebayItemIdRaw) || ebayItemIdFromAny(ebayActiveItemIdRaw);
  const ebayRawItemUrl = soldBasisRequiresSoldUrl
    ? (soldReferenceLinkAvailable
      ? soldItemUrl
      : pick(meta, ["market_item_url_active", "market_item_url", "ebay_item_url", "market_url"], null))
    : ((hasSoldItemReference && soldItemUrl)
      ? soldItemUrl
      : pick(meta, ["market_item_url_active", "ebay_item_url", "market_item_url", "market_url"], null));
  const ebayItemUrl = ebayRawItemUrl
    ? canonicalEbayItemUrl(ebayRawItemUrl, ebayItemIdRaw || ebayActiveItemIdRaw)
    : (ebayItemId ? `https://www.ebay.com/itm/${ebayItemId}` : null);

  return {
    meta,
    fxRate,
    ebay: {
      title: (hasSoldItemReference && soldItemTitle) ? soldItemTitle : (candidate.market_title || "-"),
      site: candidate.market_site || "ebay",
      itemId: ebayItemId || ebayItemIdRaw || ebayActiveItemIdRaw,
      imageUrl: (hasSoldItemReference && soldImageUrl) ? soldImageUrl : pickImage(meta, "ebay"),
      itemUrl: ebayItemUrl,
      condition: String(pick(meta, ["market_condition"], candidate.condition || "") || ""),
      identifiers: marketIdentifiers,
      priceUsd: ebayPrice,
      shippingUsd: ebayShipping,
      totalUsd: ebayPrice === null ? null : ebayTotal,
      soldCount90d,
      soldMin90d,
      saleBasisType,
      isSoldItemReference: hasSoldItemReference,
      soldReferenceLinkAvailable,
      soldBasisRequiresSoldUrl,
    },
    jp: {
      title: candidate.source_title || "-",
      site: candidate.source_site || "japan",
      itemId: String(candidate.source_item_id || "").trim(),
      imageUrl: pickImage(meta, "jp"),
      itemUrl: pick(meta, ["jp_item_url", "source_item_url", "source_url"], null),
      condition: String(pick(meta, ["source_condition"], candidate.condition || "") || ""),
      identifiers: sourceIdentifiers,
      priceJpy: jpPrice,
      shippingJpy: jpShipping,
      totalJpy: jpPrice === null ? null : jpTotalJpy,
      totalUsd: jpPrice === null || !Number.isFinite(jpTotalUsd) ? null : jpTotalUsd,
      requireInStock: Boolean(meta.source_require_in_stock ?? true),
      stockStatus: sourceStockStatus || "",
    },
    expectedProfitUsd,
    expectedProfitJpy,
    expectedMarginRate: toNumber(candidate.expected_margin_rate),
    liquidity,
    ev90: (meta && typeof meta.ev90 === "object") ? meta.ev90 : null,
    calc: {
      input: calcInput,
      breakdown: calcBreakdown,
      revenueUsd,
      jpCostUsd,
      variableFeeUsd,
      usdCostTotal,
      expensesUsd,
      grossDiffUsd,
      intlShippingUsd,
      customsUsd,
      packagingUsd,
      fixedFeeUsd,
      miscCostUsd,
    },
  };
}

function phaseToJa(phase) {
  const key = String(phase || "").trim().toLowerCase();
  const map = {
    idle: "待機中",
    running: "実行中",
    skipped: "スキップ",
    cooldown_skip: "クールダウン中",
    starting: "起動中",
    startup: "準備中",
    login_url_loaded: "画面遷移",
    login_pause_completed: "ログイン待機完了",
    query_start: "検索開始",
    search_done: "検索完了",
    filters_applying: "条件設定中",
    filters_done: "条件設定完了",
    query_done: "検索完了",
    timed_fetch_start: "探索開始",
    seed_pool_ready: "Seed準備完了",
    pass_running: "探索中",
    stage1_running: "一次判定",
    stage2_running: "最終再判定",
    pass_completed: "集計完了",
    timed_fetch_finalize: "結果集計中",
    single_pass_running: "探索中",
    completed: "完了",
    stopped: "停止",
    timeout: "タイムアウト",
    failed: "失敗",
    error: "エラー",
    daily_limit_reached: "上限到達",
  };
  return map[key] || key || "進行中";
}

function compactQueryText(raw, maxLen = 22) {
  const text = String(raw || "").trim();
  if (!text) return "";
  if (text.length <= maxLen) return text;
  return `${text.slice(0, maxLen - 1)}…`;
}

function clampPercent(raw) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return 0;
  return Math.min(100, Math.max(0, n));
}

function setFetchHeadline(text, { warn = false, running = false } = {}) {
  if (!refs.fetchStatusHeadline) return;
  refs.fetchStatusHeadline.textContent = String(text || "");
  refs.fetchStatusHeadline.classList.toggle("warn", Boolean(warn));
  refs.fetchStatusHeadline.classList.toggle("running", Boolean(running));
}

function renderRpaProgress(snapshot, { running = false } = {}) {
  if (!refs.rpaProgressWrap) return;
  const data = (snapshot && typeof snapshot === "object") ? snapshot : {};
  const percent = clampPercent(data.progress_percent);
  const status = String(data.status || "").trim().toLowerCase();
  const phase = String(data.phase || "").trim();
  const message = String(data.message || "").trim();
  const query = String(data.query || "").trim();
  const passIndex = Number(data.pass_index || 0);
  const maxPasses = Number(data.max_passes || 0);
  const createdCount = Number(data.created_count || 0);
  const currentSeedQuery = String(data.current_seed_query || "").trim();
  const stage1PassTotal = Number(data.stage1_pass_total || 0);
  const stage2Runs = Number(data.stage2_runs || 0);
  const stage1SkipTopReason = String(data.stage1_skip_top_reason || "").trim();
  const stage1SkipTopCount = Number(data.stage1_skip_top_count || 0);
  const stage1BaselineRejectTotal = Number(data.stage1_seed_baseline_reject_total || 0);
  const skippedLowQualityCount = Number(data.skipped_low_quality_count || 0);
  const elapsedSec = Number(data.elapsed_sec || 0);
  const rpa = (data.rpa && typeof data.rpa === "object") ? data.rpa : null;
  const qIndex = Number(data.query_index || 0);
  const qTotal = Number(data.total_queries || 0);
  const updatedAgoSec = Number(data.updated_ago_sec || 0);
  const runLabel = (passIndex > 0 && maxPasses > 0)
    ? ` (${passIndex}/${maxPasses})`
    : ((qIndex > 0 && qTotal > 0) ? ` (${qIndex}/${qTotal})` : "");
  const statusJa = phaseToJa(phase || status);
  const detailBits = [];
  if (currentSeedQuery) detailBits.push(`処理中Seed:${compactQueryText(currentSeedQuery, 30)}`);
  else if (query) detailBits.push(`探索語:${compactQueryText(query, 30)}`);
  if (Number.isFinite(stage1PassTotal) && stage1PassTotal > 0) detailBits.push(`一次通過:${stage1PassTotal}件`);
  if (Number.isFinite(stage2Runs) && stage2Runs > 0) detailBits.push(`最終再判定:${stage2Runs}件`);
  if (Number.isFinite(createdCount) && createdCount > 0) detailBits.push(`候補:${createdCount}件`);
  if (stage1SkipTopReason && Number.isFinite(stage1SkipTopCount) && stage1SkipTopCount > 0) {
    detailBits.push(`除外トップ:${skipReasonLabel(stage1SkipTopReason)} ${stage1SkipTopCount}件`);
  }
  if (rpa && typeof rpa === "object") {
    const rpaStatus = String(rpa.status || "").trim().toLowerCase();
    const rpaPhase = String(rpa.phase || "").trim();
    const rpaPhaseJa = phaseToJa(rpaPhase || rpaStatus);
    const rpaPct = clampPercent(rpa.progress_percent);
    const rpaQueryIndex = Number(rpa.query_index || 0);
    const rpaTotalQueries = Number(rpa.total_queries || 0);
    const rpaUpdatedAgoSec = Number(rpa.updated_ago_sec || -1);
    const rpaFresh = Number.isFinite(rpaUpdatedAgoSec) ? (rpaUpdatedAgoSec >= 0 && rpaUpdatedAgoSec <= 120) : true;
    // 前回runの「completed 100%」を引きずらないよう、実行中かつ新しい更新だけ表示する。
    if (rpaStatus === "running" && rpaFresh) {
      const rpaRunLabel = rpaQueryIndex > 0 && rpaTotalQueries > 0 ? `(${rpaQueryIndex}/${rpaTotalQueries})` : "";
      detailBits.push(`PR:${rpaPhaseJa}${rpaRunLabel} ${Math.round(rpaPct)}%`);
    }
  }
  if (Number.isFinite(stage1BaselineRejectTotal) && stage1BaselineRejectTotal > 0) {
    detailBits.push(`一次価格除外:${stage1BaselineRejectTotal}件`);
  }
  if (Number.isFinite(skippedLowQualityCount) && skippedLowQualityCount > 0) {
    detailBits.push(`低品質除外:${skippedLowQualityCount}件`);
  }
  if (!detailBits.length && message) detailBits.push(compactQueryText(message, 36));
  if (Number.isFinite(elapsedSec) && elapsedSec > 0) detailBits.push(`経過:${Math.round(elapsedSec)}s`);
  if (Number.isFinite(updatedAgoSec) && updatedAgoSec >= 0 && !running) detailBits.push(`更新:${updatedAgoSec}s`);
  const detail = detailBits.join(" / ") || "進捗データ待機中";

  refs.rpaProgressWrap.hidden = false;
  if (refs.rpaProgressLabel) refs.rpaProgressLabel.textContent = `${statusJa}${runLabel}`;
  if (refs.rpaProgressPercent) refs.rpaProgressPercent.textContent = `${Math.round(percent)}%`;
  if (refs.rpaProgressFill) refs.rpaProgressFill.style.width = `${percent.toFixed(1)}%`;
  if (refs.rpaProgressDetail) refs.rpaProgressDetail.textContent = detail;

  if (running) {
    if (refs.fetchBtn && refs.fetchBtn.disabled) {
      refs.fetchBtn.textContent = `探索中... ${Math.round(percent)}%`;
    }
    const runText = runLabel ? runLabel.replace(/[()]/g, "") : "";
    const head = runText ? `探索中 ${Math.round(percent)}% / ${runText}` : `探索中 ${Math.round(percent)}%`;
    setFetchHeadline(head, {
      warn: Boolean(data.daily_limit_reached),
      running: true,
    });
  }
}

function hideRpaProgress() {
  if (!refs.rpaProgressWrap) return;
  refs.rpaProgressWrap.hidden = true;
}

async function pollRpaProgressOnce() {
  try {
    let snap = null;
    let source = "fetch-progress";
    try {
      snap = await api("/v1/system/fetch-progress");
    } catch (_) {
      const rpaOnly = await api("/v1/system/rpa-progress");
      source = "rpa-progress-fallback";
      snap = {
        status: state.fetchInFlight ? "running" : "idle",
        phase: state.fetchInFlight ? "running" : "idle",
        message: "探索進捗の取得待ち（RPAのみ受信）",
        progress_percent: state.displayedProgress,
        query: "",
        pass_index: 0,
        max_passes: 0,
        created_count: 0,
        updated_ago_sec: Number(rpaOnly?.updated_ago_sec || 0),
        rpa: rpaOnly,
      };
    }
    const status = String(snap?.status || "").trim().toLowerCase();
    const runId = String(snap?.run_id || "").trim();
    const updatedAtEpoch = Number(snap?.updated_at_epoch || 0);
    const isFreshForCurrentFetch = updatedAtEpoch > 0 && updatedAtEpoch >= (state.fetchStartedAtEpochSec - 1);
    let staleSnapshot = false;

    if (state.fetchInFlight) {
      if (!state.fetchProgressSawRunning) {
        if (status === "running" && isFreshForCurrentFetch) {
          state.fetchProgressSawRunning = true;
          if (runId) state.fetchProgressRunId = runId;
        } else {
          staleSnapshot = true;
        }
      } else {
        if (!state.fetchProgressRunId && runId && status === "running") {
          state.fetchProgressRunId = runId;
        }
        if (state.fetchProgressRunId && runId && runId !== state.fetchProgressRunId) {
          staleSnapshot = true;
        }
      }
    }

    const now = Date.now();
    const elapsedSec = state.fetchStartedAtMs > 0 ? (now - state.fetchStartedAtMs) / 1000 : 0;
    const optimistic = Math.min(92, 4 + elapsedSec * 2.05);
    const rawServerPercent = Number.isFinite(Number(snap?.progress_percent))
      ? clampPercent(Number(snap?.progress_percent))
      : 0;
    const serverPercent = staleSnapshot ? 0 : rawServerPercent;
    let targetPercent = serverPercent;
    if (state.fetchInFlight) {
      targetPercent = Math.max(serverPercent, optimistic);
      // 実行中は逆戻りさせない。
      targetPercent = Math.max(targetPercent, state.optimisticProgress);
    }
    state.optimisticProgress = clampPercent(targetPercent);
    const prevDisplay = clampPercent(state.displayedProgress);
    let displayPercent = state.optimisticProgress;
    if (state.fetchInFlight) {
      if (displayPercent < prevDisplay) displayPercent = prevDisplay;
      // 急なジャンプを軽減し、段階的に追随させる。
      if (displayPercent > prevDisplay) {
        const delta = displayPercent - prevDisplay;
        const step = Math.max(1.4, Math.min(8.0, delta * 0.55));
        displayPercent = Math.min(displayPercent, prevDisplay + step);
      }
    }
    state.displayedProgress = clampPercent(displayPercent);
    const merged = {
      ...snap,
      progress_percent: state.displayedProgress,
    };
    if (state.fetchInFlight && staleSnapshot) {
      merged.status = "running";
      merged.phase = "startup";
      merged.message = "探索リクエスト送信中（前回進捗を無視）";
    }
    state.lastFetchProgressSource = source;
    renderRpaProgress(merged, { running: state.fetchInFlight });
    if (!state.fetchInFlight && ["completed", "failed", "stopped", "idle"].includes(status)) {
      stopRpaProgressPolling();
    }
  } catch (_) {
    if (state.fetchInFlight) {
      const now = Date.now();
      const elapsedSec = state.fetchStartedAtMs > 0 ? (now - state.fetchStartedAtMs) / 1000 : 0;
      const optimistic = Math.min(82, 4 + elapsedSec * 1.9);
      state.optimisticProgress = Math.max(state.optimisticProgress, optimistic);
      state.displayedProgress = Math.max(state.displayedProgress, Math.min(state.optimisticProgress, state.displayedProgress + 2.2));
      renderRpaProgress(
        {
          status: "running",
          phase: "starting",
          message: "探索処理中（進捗取得待ち）",
          progress_percent: state.displayedProgress,
          query_index: 0,
          total_queries: 0,
          updated_ago_sec: 0,
        },
        { running: true }
      );
    }
  }
}

function startRpaProgressPolling() {
  stopRpaProgressPolling();
  state.fetchInFlight = true;
  state.fetchStartedAtMs = Date.now();
  state.fetchStartedAtEpochSec = Math.floor(state.fetchStartedAtMs / 1000);
  state.fetchProgressRunId = "";
  state.fetchProgressSawRunning = false;
  state.optimisticProgress = 2;
  state.displayedProgress = 2;
  state.lastFetchProgressSource = "";
  renderRpaProgress(
    {
      status: "running",
      phase: "startup",
      message: "探索全体を開始しました",
      progress_percent: state.displayedProgress,
      query_index: 0,
      total_queries: 0,
      updated_ago_sec: 0,
    },
    { running: true }
  );
  state.rpaProgressPollTimer = window.setInterval(() => {
    void pollRpaProgressOnce();
  }, 850);
  void pollRpaProgressOnce();
}

function stopRpaProgressPolling({ finalMessage = "" } = {}) {
  state.fetchInFlight = false;
  state.fetchProgressSawRunning = false;
  state.fetchProgressRunId = "";
  if (state.rpaProgressPollTimer) {
    window.clearInterval(state.rpaProgressPollTimer);
    state.rpaProgressPollTimer = null;
  }
  if (finalMessage) {
    setFetchHeadline(finalMessage, { running: false });
  } else if (refs.fetchStatusHeadline) {
    refs.fetchStatusHeadline.classList.remove("running");
  }
}

function renderFetchStats(payload) {
  if (!refs.fetchStatusHeadline || !refs.fetchStatsRows) return;
  if (!payload || typeof payload !== "object") {
    setFetchHeadline("まだ探索を実行していません。", { warn: false, running: false });
    renderHeaderSeedStatus(null, { loading: false, failed: false });
    renderSeedPoolSummary(null);
    refs.fetchStatsRows.innerHTML = "";
    hideRpaProgress();
    return;
  }
  renderHeaderSeedStatus(payload, { loading: false, failed: false });
  renderSeedPoolSummary(payload);

  const createdCount = Number(payload.created_count || 0);
  const searchScopeDone = Boolean(payload.search_scope_done);
  const queryCacheSkip = Boolean(payload.query_cache_skip);
  const errors = Array.isArray(payload.errors) ? payload.errors : [];
  const fetched = (payload.fetched && typeof payload.fetched === "object") ? payload.fetched : {};
  const dailyLimitReached = isRpaDailyLimitReached(payload);

  let headline = `今回の探索: 追加 ${createdCount}件`;
  if (searchScopeDone) headline += " / 探索完走";
  if (queryCacheSkip) headline += " / 同条件スキップ";
  if (dailyLimitReached) headline += " / Product Research上限到達";
  if (errors.length > 0) headline += ` / エラー${errors.length}件`;
  setFetchHeadline(headline, { warn: dailyLimitReached, running: false });

  const rows = Object.entries(fetched);
  const siteRows = rows.filter(([site]) => {
    const key = String(site || "").trim().toLowerCase();
    return key === "ebay" || key === "rakuten" || key === "yahoo" || key === "yahoo_shopping" || key === "amazon";
  });
  if (siteRows.length === 0) {
    const alertHtml = dailyLimitReached
      ? `
      <div class="fetch-alert warn">
        Product Researchの1日上限に到達したため、探索を停止しました。翌日に再開してください。
      </div>
    `
      : "";
    refs.fetchStatsRows.innerHTML = `
      ${alertHtml}
      <div class="fetch-row">
        <div class="head"><span>探索ログなし</span><span class="fetch-stop">-</span></div>
        <p class="fetch-note">この探索ではサイト別の取得データがありませんでした。</p>
      </div>
    `;
    return;
  }

  const warningBlock = dailyLimitReached
    ? `
      <div class="fetch-alert warn">
        Product Researchの1日上限に到達したため、探索を停止しました。翌日に再開してください。
      </div>
    `
    : "";

  refs.fetchStatsRows.innerHTML = warningBlock + siteRows.map(([site, info]) => {
    const calls = Number(info?.calls_made || 0);
    const networkCalls = Number(info?.network_calls || 0);
    const cacheHits = Number(info?.cache_hits || 0);
    const count = Number(info?.count || 0);
    const stop = stopReasonLabel(info?.stop_reason);
    const stopDetail = stopReasonDetail(info);
    const budgetRemaining = Number(info?.budget_remaining ?? -1);
    const budgetText = budgetRemaining >= 0 ? `予算残 ${budgetRemaining}` : "予算未設定";
    const knowledge = (info && typeof info.knowledge === "object") ? info.knowledge : null;
    const knowledgeText = (knowledge && knowledge.applied)
      ? `${knowledge.category_name || knowledge.category_key || "カテゴリ"} / 展開${Number(knowledge.query_count || 0)}件`
      : "未適用";
    const efficiency = networkCalls > 0 ? `${(count / networkCalls).toFixed(1)}件/API` : "-";
    const modelBackfill = site === "ebay" && info && typeof info.model_backfill === "object" ? info.model_backfill : null;
    const modelBackfillText = modelBackfill && modelBackfill.ran
      ? ` / 型番バックフィル ${Number(modelBackfill.unique_added_items ?? modelBackfill.added_items ?? 0)}件追加`
      : "";
    return `
      <div class="fetch-row">
        <div class="head">
          <span>${escapeHtml(labelForSite(site))}</span>
          <span class="fetch-stop">${escapeHtml(stop)}</span>
        </div>
        <ul class="fetch-list">
          <li>取得件数 ${count}件 / 実API ${networkCalls}回 / 効率 ${escapeHtml(efficiency)}</li>
          <li>総呼出 ${calls}回 / キャッシュ ${cacheHits}回</li>
        </ul>
        <p class="fetch-note">${escapeHtml(budgetText)} / ナレッジ: ${escapeHtml(knowledgeText)}${escapeHtml(modelBackfillText)}</p>
        ${stopDetail ? `<p class="fetch-note">${escapeHtml(stopDetail)}</p>` : ""}
      </div>
    `;
  }).join("");
}

async function refreshSeedPoolStatusForCurrentCategory({ updateHeadline = true } = {}) {
  const category = String(refs.fetchQuery?.value || "").trim();
  if (!category) return;
  const seq = ++state.seedPoolStatusSeq;
  renderHeaderSeedStatus(null, { loading: true });
  if (!state.fetchInFlight && !state.lastFetch && refs.seedPoolSummary) {
    refs.seedPoolSummary.classList.remove("warn");
    refs.seedPoolSummary.textContent = "Seedプール情報を取得しています。";
  }
  try {
    const payload = await api(`/v1/miner/seed-pool-status?category=${encodeURIComponent(category)}`);
    if (seq !== state.seedPoolStatusSeq) return;
    state.lastSeedPoolCategory = category;
    renderHeaderSeedStatus(payload, { loading: false, failed: false });
    renderSeedPoolSummary(payload);
    if (updateHeadline && !state.fetchInFlight) {
      const view = getSeedPoolView(payload);
      setFetchHeadline(
        `探索前: ${view.categoryLabel} / Seed数 ${view.seedCount}件`,
        { warn: Boolean(view.dailyLimitReached), running: false },
      );
    }
  } catch (_) {
    if (seq !== state.seedPoolStatusSeq) return;
    renderHeaderSeedStatus(null, { loading: false, failed: true });
    if (refs.seedPoolSummary) {
      refs.seedPoolSummary.classList.add("warn");
      refs.seedPoolSummary.textContent = "Seedプール情報の取得に失敗しました。探索時に再取得します。";
    }
    if (updateHeadline && !state.fetchInFlight && !state.lastFetch) {
      setFetchHeadline("探索前のSeedプール情報を取得できませんでした。", { warn: true, running: false });
    }
  }
}

function renderImage(imgEl, url, clickHref = null, title = "") {
  if (url) {
    imgEl.src = url;
    imgEl.alt = "商品画像";
  } else {
    imgEl.removeAttribute("src");
    imgEl.alt = "画像未取得";
  }
  const href = String(clickHref || "").trim();
  if (href) {
    imgEl.classList.add("clickable");
    imgEl.tabIndex = 0;
    imgEl.setAttribute("role", "link");
    imgEl.title = title ? `${title} を開く` : "商品ページを開く";
    imgEl.onclick = () => window.open(href, "_blank", "noopener,noreferrer");
    imgEl.onkeydown = (ev) => {
      if (ev.key === "Enter" || ev.key === " ") {
        ev.preventDefault();
        window.open(href, "_blank", "noopener,noreferrer");
      }
    };
  } else {
    imgEl.classList.remove("clickable");
    imgEl.removeAttribute("tabindex");
    imgEl.removeAttribute("role");
    imgEl.removeAttribute("title");
    imgEl.onclick = null;
    imgEl.onkeydown = null;
  }
}

function buildSearchUrl(site, title) {
  const q = encodeURIComponent(String(title || "").trim());
  const normalized = String(site || "").toLowerCase();
  if (!q) return null;
  if (normalized === "ebay") return `https://www.ebay.com/sch/i.html?_nkw=${q}`;
  if (normalized === "rakuten") return `https://search.rakuten.co.jp/search/mall/${q}/`;
  if (normalized === "yahoo" || normalized === "yahoo_shopping") return `https://shopping.yahoo.co.jp/search?p=${q}`;
  if (normalized === "amazon") return `https://www.amazon.co.jp/s?k=${q}`;
  return null;
}

function isLikelyInvalidEbayItemUrl(url) {
  try {
    const u = new URL(url);
    if (!u.hostname.includes("ebay.")) return false;
    if (u.hostname.includes("example.")) return true;
    const path = u.pathname || "";
    const m = path.match(/\/itm\/(?:[^/]+\/)?(\d{9,15})/);
    if (!m) {
      // 検索/リサーチ一覧は商品ページとしては無効扱いにする
      return /\/sch\/i\.html|\/sh\/research|\/srp\//i.test(path);
    }
    const itemId = m[1] || "";
    return itemId.length < 9 || itemId.length > 15;
  } catch {
    return true;
  }
}

function ebayItemIdFromAny(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const m = text.match(/(?:^|[|/])(\d{9,15})(?:$|[|/?#&])/);
  return m ? String(m[1] || "") : "";
}

function canonicalEbayItemUrl(url, itemIdHint = "") {
  const raw = String(url || "").trim();
  const hintId = ebayItemIdFromAny(itemIdHint);
  try {
    const u = new URL(raw);
    if (!u.hostname.includes("ebay.")) return raw;
    const id = ebayItemIdFromAny(u.pathname || "") || hintId;
    if (id) return `https://www.ebay.com/itm/${id}`;
    return raw;
  } catch {
    if (hintId) return `https://www.ebay.com/itm/${hintId}`;
    return raw;
  }
}

function resolveDisplayLink(url, site, title, { allowFallback = true, itemIdHint = "" } = {}) {
  const raw = String(url || "").trim();
  const siteKey = String(site || "").toLowerCase();
  const ebayHintId = siteKey === "ebay" ? ebayItemIdFromAny(itemIdHint) : "";
  if (!raw) {
    if (siteKey === "ebay" && ebayHintId) {
      return { href: `https://www.ebay.com/itm/${ebayHintId}`, fallbackUsed: false };
    }
    return { href: allowFallback ? buildSearchUrl(site, title) : null, fallbackUsed: true };
  }
  try {
    const u = new URL(raw);
    if (!/^https?:$/.test(u.protocol)) {
      if (siteKey === "ebay" && ebayHintId) {
        return { href: `https://www.ebay.com/itm/${ebayHintId}`, fallbackUsed: false };
      }
      return { href: allowFallback ? buildSearchUrl(site, title) : null, fallbackUsed: true };
    }
    if (siteKey === "ebay") {
      if (isLikelyInvalidEbayItemUrl(raw)) {
        if (ebayHintId) {
          return { href: `https://www.ebay.com/itm/${ebayHintId}`, fallbackUsed: false };
        }
        return { href: allowFallback ? buildSearchUrl(site, title) : null, fallbackUsed: true };
      }
      const canonical = canonicalEbayItemUrl(raw, itemIdHint);
      return { href: canonical || raw, fallbackUsed: false };
    }
    return { href: raw, fallbackUsed: false };
  } catch {
    if (siteKey === "ebay" && ebayHintId) {
      return { href: `https://www.ebay.com/itm/${ebayHintId}`, fallbackUsed: false };
    }
    return { href: allowFallback ? buildSearchUrl(site, title) : null, fallbackUsed: true };
  }
}

function renderLink(aEl, url, site, title, options = {}) {
  const resolved = resolveDisplayLink(url, site, title, options);
  if (resolved.href) {
    aEl.href = resolved.href;
    aEl.style.pointerEvents = "auto";
    aEl.style.opacity = "1";
    aEl.textContent = resolved.fallbackUsed ? "検索結果を開く（予備）" : "商品ページを開く";
  } else {
    aEl.removeAttribute("href");
    aEl.style.pointerEvents = "none";
    aEl.style.opacity = "0.45";
    aEl.textContent = "商品ページURL未取得";
  }
  return resolved.href || null;
}

function renderCandidate(candidate) {
  if (!candidate) {
    refs.ebayTitle.textContent = "候補がありません";
    refs.jpTitle.textContent = "候補がありません";
    refs.ebayPrice.textContent = "-";
    refs.ebayShipping.textContent = "-";
    refs.ebayTotal.textContent = "-";
    if (refs.ebayExtraCosts) refs.ebayExtraCosts.textContent = "-";
    refs.ebaySoldCount90d.textContent = "-";
    refs.ebaySoldMin90d.textContent = "-";
    refs.jpPrice.textContent = "-";
    refs.jpShipping.textContent = "-";
    refs.jpTotal.textContent = "-";
    if (refs.jpStockRow) refs.jpStockRow.style.display = "none";
    refs.jpStockRule.textContent = "-";
    if (refs.jpExtraInfo) refs.jpExtraInfo.textContent = "-";
    refs.fxRate.textContent = "-";
    refs.sumRevenue.textContent = "-";
    setOptionalNote(refs.sumRevenueBreakdown, "-", "-");
    refs.sumPurchase.textContent = "-";
    setOptionalNote(refs.sumPurchaseBreakdown, "-", "-");
    refs.sumExpenses.textContent = "-";
    setOptionalNote(refs.sumExpensesBreakdown, "-", "-");
    refs.sumProfit.textContent = "-";
    setOptionalNote(refs.sumProfitBreakdown, "-", "-");
    if (refs.financeFormula) refs.financeFormula.textContent = "最終利益 = 売上見込み - 仕入原価 - 諸経費合計";
    refs.sumSoldCount90d.textContent = "-";
    refs.sumSoldMin90d.textContent = "-";
    if (refs.sumLiquidityGate) refs.sumLiquidityGate.textContent = "-";
    if (refs.sumFeeRates) refs.sumFeeRates.textContent = "-";
    if (refs.sumOtherCosts) refs.sumOtherCosts.textContent = "-";
    if (refs.decisionReasons) refs.decisionReasons.innerHTML = '<span class="reason-chip">理由を判定中</span>';
    setDecisionSummary({ label: "-", sub: "候補を選択してください", tone: "info" });
    renderExtracted(refs.ebayExtracted, [
      { key: "メーカー", val: "-" },
      { key: "型番", val: "-" },
      { key: "色", val: "-" },
      { key: "状態", val: "-" },
      { key: "その他", val: "-" },
    ]);
    renderExtracted(refs.jpExtracted, [
      { key: "メーカー", val: "-" },
      { key: "型番", val: "-" },
      { key: "色", val: "-" },
      { key: "状態", val: "-" },
      { key: "その他", val: "-" },
    ]);
    updateIssueTargetHighlights(null);
    refs.riskFlags.innerHTML = "";
    const emptyEbayHref = renderLink(refs.ebayLink, null, "ebay", "");
    const emptyJpHref = renderLink(refs.jpLink, null, "japan", "");
    renderImage(refs.ebayImage, null, emptyEbayHref, "");
    renderImage(refs.jpImage, null, emptyJpHref, "");
    if (refs.calcDigest) refs.calcDigest.textContent = "候補を選択してください。";
    refs.calcData.innerHTML = "";
    refs.rawJson.textContent = "";
    refs.approveBtn.disabled = true;
    refs.rejectBtn.disabled = true;
    refs.approveBtn.textContent = "同一商品・利益OK（承認）";
    setApproveHint("承認するとダミー出品ステータスに遷移します。");
    refs.currentCandidateLabel.textContent = "候補を選択してください";
    markActiveCandidateInList(null);
    scheduleFinanceCellHeightSync();
    return;
  }

  const v = normalize(candidate);
  const status = String(candidate.status || "").toLowerCase();
  const autoReview = (v.meta && typeof v.meta.auto_miner === "object") ? v.meta.auto_miner : null;
  const colorRisk = getColorRiskInfo(candidate);
  const latestRejection = getLatestRejection(candidate);
  const isAutoApproved = status === "approved" && Boolean(autoReview?.approved);

  refs.currentCandidateLabel.textContent = `選択中: #${candidate.id} (${labelForStatus(candidate.status)})`;
  refs.ebaySiteTag.textContent = v.ebay.isSoldItemReference
    ? `${labelForSite(v.ebay.site || "ebay")}（売却済み）`
    : labelForSite(v.ebay.site || "ebay");
  refs.jpSiteTag.textContent = labelForSite(v.jp.site || "japan");
  refs.ebayTitle.textContent = v.ebay.title;
  refs.jpTitle.textContent = v.jp.title;
  const marketExtracted = buildExtractedSnapshot(v.ebay);
  const sourceExtracted = buildExtractedSnapshot(v.jp);
  renderExtracted(refs.ebayExtracted, buildExtractedFields(marketExtracted));
  renderExtracted(refs.jpExtracted, buildExtractedFields(sourceExtracted));
  updateIssueTargetHighlights({ source: sourceExtracted, market: marketExtracted });
  setMoneyCell(refs.ebayPrice, { jpy: null, usd: v.ebay.priceUsd, fxRate: v.fxRate });
  setShippingCell(refs.ebayShipping, { jpy: null, usd: v.ebay.shippingUsd, fxRate: v.fxRate });
  setMoneyCell(refs.ebayTotal, { jpy: null, usd: v.ebay.totalUsd, fxRate: v.fxRate });
  if (refs.ebayExtraCosts) {
    const intl = toNumber(v.calc?.input?.international_shipping_usd);
    const customs = toNumber(v.calc?.input?.customs_usd);
    const packaging = toNumber(v.calc?.input?.packaging_usd);
    const extras = [];
    if (Number.isFinite(intl)) extras.push(`国際送料 ${formatUsd(intl)}`);
    if (Number.isFinite(customs)) extras.push(`関税 ${formatUsd(customs)}`);
    if (Number.isFinite(packaging)) extras.push(`梱包 ${formatUsd(packaging)}`);
    refs.ebayExtraCosts.textContent = extras.length ? extras.join(" / ") : "未取得";
  }
  refs.ebaySoldCount90d.textContent = Number.isFinite(v.ebay.soldCount90d) && v.ebay.soldCount90d >= 0
    ? `${v.ebay.soldCount90d}件`
    : "未取得";
  setMoneyCell(refs.ebaySoldMin90d, { jpy: null, usd: v.ebay.soldMin90d, fxRate: v.fxRate });
  setMoneyCell(refs.jpPrice, { jpy: v.jp.priceJpy, usd: null, fxRate: v.fxRate });
  setShippingCell(refs.jpShipping, { jpy: v.jp.shippingJpy, usd: null, fxRate: v.fxRate });
  setMoneyCell(refs.jpTotal, { jpy: v.jp.totalJpy, usd: v.jp.totalUsd, fxRate: v.fxRate });
  if (refs.jpStockRow) {
    const hasStock = Boolean(v.jp.stockStatus);
    refs.jpStockRow.style.display = hasStock ? "" : "none";
  }
  refs.jpStockRule.textContent = v.jp.stockStatus || "-";
  if (refs.jpExtraInfo) {
    refs.jpExtraInfo.textContent = Number.isFinite(v.jp.shippingJpy) && v.jp.shippingJpy === 0
      ? "国内送料込み"
      : "国内送料別";
  }

  const ebayHref = renderLink(
    refs.ebayLink,
    v.ebay.itemUrl,
    v.ebay.site,
    v.ebay.title,
    { allowFallback: !v.ebay.soldBasisRequiresSoldUrl, itemIdHint: v.ebay.itemId }
  );
  const jpHref = renderLink(refs.jpLink, v.jp.itemUrl, v.jp.site, v.jp.title);
  renderImage(refs.ebayImage, v.ebay.imageUrl, ebayHref, v.ebay.title);
  renderImage(refs.jpImage, v.jp.imageUrl, jpHref, v.jp.title);

  refs.fxRate.textContent = v.fxRate ? `1 USD = ${v.fxRate.toFixed(4)} JPY` : "-";
  const revenueUsd = toNumber(v.calc?.revenueUsd ?? v.ebay.totalUsd);
  const purchaseUsd = toNumber(v.calc?.jpCostUsd ?? v.jp.totalUsd);
  const expenseUsd = toNumber(v.calc?.expensesUsd);
  const grossDiffUsd = toNumber(v.calc?.grossDiffUsd);
  const costTotalUsd = toNumber(v.calc?.usdCostTotal);

  setMoneyCell(refs.sumRevenue, { jpy: null, usd: revenueUsd, fxRate: v.fxRate });
  if (refs.sumRevenueBreakdown) {
    const lines = [
      summaryMoneyLineHtml("商品", { jpy: null, usd: v.ebay.priceUsd, fxRate: v.fxRate }),
      `<span class="summary-line summary-line-money"><span class="summary-line-label">送料</span>${shippingInlineHtml({ jpy: null, usd: v.ebay.shippingUsd, fxRate: v.fxRate })}</span>`,
    ];
    const hasSpecialBasis = String(v.ebay.saleBasisType || "").toLowerCase() !== "active_listing_price";
    if (hasSpecialBasis) lines.push(`売値基準: ${escapeHtml(saleBasisLabel(v.ebay.saleBasisType))}`);
    setOptionalNoteHtml(refs.sumRevenueBreakdown, summaryLinesHtml(lines));
  }
  setMoneyCell(refs.sumPurchase, { jpy: v.jp.totalJpy, usd: purchaseUsd, fxRate: v.fxRate });
  if (refs.sumPurchaseBreakdown) {
    const lines = [
      summaryMoneyLineHtml("商品", { jpy: v.jp.priceJpy, usd: null, fxRate: v.fxRate }),
      `<span class="summary-line summary-line-money"><span class="summary-line-label">国内送料</span>${shippingInlineHtml({ jpy: v.jp.shippingJpy, usd: null, fxRate: v.fxRate })}</span>`,
    ];
    setOptionalNoteHtml(refs.sumPurchaseBreakdown, summaryLinesHtml(lines));
  }
  setMoneyCell(refs.sumExpenses, { jpy: null, usd: expenseUsd, fxRate: v.fxRate });
  if (refs.sumExpensesBreakdown) {
    const rows = [];
    const addExpenseRow = (label, usdVal) => {
      if (!Number.isFinite(usdVal) || usdVal <= 0) return;
      rows.push(summaryMoneyLineHtml(label, { jpy: null, usd: usdVal, fxRate: v.fxRate }));
    };
    addExpenseRow("変動手数料", toNumber(v.calc?.variableFeeUsd));
    addExpenseRow("国際送料", toNumber(v.calc?.intlShippingUsd));
    addExpenseRow("関税", toNumber(v.calc?.customsUsd));
    addExpenseRow("梱包", toNumber(v.calc?.packagingUsd));
    addExpenseRow("固定費", toNumber(v.calc?.fixedFeeUsd));
    addExpenseRow("その他", toNumber(v.calc?.miscCostUsd));
    if (rows.length === 0) {
      setOptionalNote(refs.sumExpensesBreakdown, "", "未取得");
    } else {
      setOptionalNoteHtml(refs.sumExpensesBreakdown, summaryLinesHtml(rows));
    }
  }
  setMoneyCell(refs.sumProfit, { jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate });
  if (refs.sumProfitBreakdown) {
    const marginText = Number.isFinite(v.expectedMarginRate) ? `粗利率 ${escapeHtml(formatPercent(v.expectedMarginRate))}` : "";
    const grossText = Number.isFinite(grossDiffUsd)
      ? summaryMoneyLineHtml("粗差額", { jpy: null, usd: grossDiffUsd, fxRate: v.fxRate })
      : "";
    const rows = [marginText, grossText].filter(Boolean);
    setOptionalNoteHtml(refs.sumProfitBreakdown, summaryLinesHtml(rows));
  }
  if (refs.financeFormula) {
    refs.financeFormula.textContent = "最終利益 = 売上見込み - 仕入原価 - 諸経費合計";
  }
  refs.sumSoldCount90d.textContent = Number.isFinite(v.ebay.soldCount90d) && v.ebay.soldCount90d >= 0
    ? `${v.ebay.soldCount90d}件`
    : "未取得";
  if (Number.isFinite(v.ebay.soldMin90d) && v.ebay.soldMin90d > 0) {
    setMoneyCell(refs.sumSoldMin90d, { jpy: null, usd: v.ebay.soldMin90d, fxRate: v.fxRate });
  } else {
    refs.sumSoldMin90d.textContent = "未取得";
  }
  if (refs.sumLiquidityGate) {
    const gatePassed = Boolean(v.liquidity?.gate_passed);
    const gateReason = String(v.liquidity?.gate_reason || "").trim();
    refs.sumLiquidityGate.textContent = gatePassed ? "通過" : `除外${gateReason ? ` (${gateReason})` : ""}`;
  }
  const feeRateMarket = toNumber(v.calc?.input?.marketplace_fee_rate);
  const feeRatePayment = toNumber(v.calc?.input?.payment_fee_rate);
  if (refs.sumFeeRates) {
    if (Number.isFinite(feeRateMarket) || Number.isFinite(feeRatePayment)) {
      const marketText = Number.isFinite(feeRateMarket) ? formatPercent(feeRateMarket) : "-";
      const paymentText = Number.isFinite(feeRatePayment) ? formatPercent(feeRatePayment) : "-";
      refs.sumFeeRates.textContent = `販売 ${marketText} / 決済 ${paymentText}`;
    } else {
      refs.sumFeeRates.textContent = "未取得";
    }
  }
  if (refs.sumOtherCosts) {
    const intl = toNumber(v.calc?.input?.international_shipping_usd);
    const customs = toNumber(v.calc?.input?.customs_usd);
    const packaging = toNumber(v.calc?.input?.packaging_usd);
    const fixedFee = toNumber(v.calc?.input?.fixed_fee_usd);
    const pieces = [];
    if (Number.isFinite(intl)) pieces.push(`国際送料 ${formatUsd(intl)}`);
    if (Number.isFinite(customs)) pieces.push(`関税 ${formatUsd(customs)}`);
    if (Number.isFinite(packaging)) pieces.push(`梱包 ${formatUsd(packaging)}`);
    if (Number.isFinite(fixedFee)) pieces.push(`固定費 ${formatUsd(fixedFee)}`);
    refs.sumOtherCosts.textContent = pieces.length ? pieces.join(" / ") : "未取得";
  }
  setDecisionSummary(buildDecisionSummary(candidate, v));
  const decisionReasons = buildDecisionReasons(candidate, v, colorRisk);
  if (refs.decisionReasons) {
    refs.decisionReasons.innerHTML = decisionReasons.length
      ? decisionReasons.map((row) => {
        const body = (typeof row.html === "string" && row.html.trim().length > 0)
          ? row.html
          : escapeHtml(row.text);
        return `<span class="reason-chip ${row.tone === "warn" ? "warn" : "good"}">${body}</span>`;
      }).join("")
      : '<span class="reason-chip">判定理由なし</span>';
  }

  if (refs.calcDigest) {
    const scoreText = toNumber(candidate.match_score)?.toFixed(3) ?? "-";
    const soldText = Number.isFinite(v.ebay.soldCount90d) && v.ebay.soldCount90d >= 0 ? `${v.ebay.soldCount90d}件` : "未取得";
    const soldMinText = Number.isFinite(v.ebay.soldMin90d) && v.ebay.soldMin90d > 0
      ? moneyDualInlineHtml({ jpy: null, usd: v.ebay.soldMin90d, fxRate: v.fxRate })
      : "未取得";
    const profitText = moneyDualHtml({ jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate, align: "left" });
    const grossDiffText = moneyDualInlineHtml({ jpy: null, usd: grossDiffUsd, fxRate: v.fxRate });
    const variableFeeText = moneyDualInlineHtml({ jpy: null, usd: toNumber(v.calc?.variableFeeUsd), fxRate: v.fxRate });
    const profitEquation = Number.isFinite(revenueUsd) && Number.isFinite(costTotalUsd)
      ? `${moneyDualInlineHtml({ jpy: null, usd: revenueUsd, fxRate: v.fxRate })} - ${moneyDualInlineHtml({ jpy: null, usd: costTotalUsd, fxRate: v.fxRate })}`
      : "-";
    refs.calcDigest.innerHTML = `
      <span class="digest-row">
        <span class="digest-chip"><strong>一致スコア</strong>${escapeHtml(scoreText)}</span>
        <span class="digest-chip"><strong>90日売却</strong>${escapeHtml(soldText)}</span>
        <span class="digest-chip"><strong>90日最低</strong>${soldMinText}</span>
        <span class="digest-chip"><strong>粗差額</strong>${grossDiffText}</span>
        <span class="digest-chip"><strong>変動手数料</strong>${variableFeeText}</span>
        <span class="digest-chip"><strong>利益式</strong>${profitEquation}</span>
        <span class="digest-chip"><strong>期待利益</strong>${profitText}</span>
        ${latestRejection ? `<span class="digest-chip"><strong>否認箇所</strong>${escapeHtml(formatIssueTargetsJa(latestRejection.issue_targets))}</span>` : ""}
      </span>
    `;
  }

  const canApprove = status === "pending" || status === "approved";
  const canReject = status === "pending" || status === "approved";
  refs.approveBtn.disabled = !canApprove;
  refs.rejectBtn.disabled = !canReject;
  refs.approveBtn.textContent = isAutoApproved
    ? "最終承認してダミー出品へ"
    : "同一商品・利益OK（承認）";

  const riskBadges = [];
  if (colorRisk.hasColorMissingRisk) riskBadges.push('<span class="risk-chip warn">色未確認マッチ</span>');
  if (isAutoApproved) riskBadges.push('<span class="risk-chip info">自動承認済み</span>');
  refs.riskFlags.innerHTML = riskBadges.join("");

  if (status === "listed") {
    refs.approveHint.classList.remove("warn");
    setApproveHint("この候補は既にダミー出品済みです。");
  } else if (isAutoApproved) {
    refs.approveHint.classList.toggle("warn", colorRisk.hasColorMissingRisk);
    if (colorRisk.hasColorMissingRisk) {
      setApproveHint("この候補は自動承認済みですが、色情報が欠損しています。最終確認後に承認してください。");
    } else {
      setApproveHint("この候補は自動承認済みです。最終チェック後に承認または否認してください。");
    }
  } else {
    refs.approveHint.classList.toggle("warn", colorRisk.hasColorMissingRisk);
    if (colorRisk.hasColorMissingRisk) {
      setApproveHint("色情報が欠損したマッチです。リンク先画像・型番・色表記を確認してから承認してください。");
    } else {
      setApproveHint("承認するとダミー出品ステータスに遷移します。");
    }
  }

  const matchRows = [
    { key: "候補ID", val: candidate.id },
    { key: "判定状態", val: labelForStatus(candidate.status) },
    { key: "一致レベル", val: candidate.match_level },
    { key: "一致スコア", val: toNumber(candidate.match_score)?.toFixed(3) ?? "-" },
    { key: "取得時マッチ理由", val: colorRisk.fetchReason || "-" },
    { key: "色未確認リスク", val: colorRisk.hasColorMissingRisk ? "あり" : "なし" },
  ];
  if (latestRejection) {
    matchRows.push({ key: "最新否認箇所", val: formatIssueTargetsJa(latestRejection.issue_targets) });
    matchRows.push({ key: "最新否認理由", val: String(latestRejection.reason_text || "").trim() || "(未入力)" });
    matchRows.push({ key: "最新否認日時", val: formatIsoShort(latestRejection.created_at || "") });
  }

  if (autoReview) {
    const autoMetrics = (typeof autoReview.metrics === "object" && autoReview.metrics) ? autoReview.metrics : {};
    matchRows.push({ key: "自動レビュー承認", val: autoReview.approved ? "あり" : "なし" });
    matchRows.push({ key: "自動レビュー承認時刻", val: formatIsoShort(autoReview.approved_at || "") });
    matchRows.push({ key: "自動レビューCycle", val: autoReview.cycle_id || "-" });
    matchRows.push({ key: "自動レビュー理由", val: autoReview.reason || "-" });
    matchRows.push({ key: "自動レビュー根拠", val: autoMetrics.confidence_tag || autoMetrics.rematch_reason || "-" });
    matchRows.push({ key: "自動レビュー再評価理由", val: colorRisk.rematchReason || "-" });
  }

  const priceRows = [
    { key: "販売収入合計", val: moneyDualHtml({ jpy: null, usd: toNumber(v.calc?.revenueUsd ?? v.ebay.totalUsd), fxRate: v.fxRate, align: "left" }), html: true },
    { key: "  └ 商品価格", val: moneyDualHtml({ jpy: null, usd: v.ebay.priceUsd, fxRate: v.fxRate, align: "left" }), html: true },
    { key: "  └ 販売時送料", val: (Number.isFinite(v.ebay.shippingUsd) && v.ebay.shippingUsd === 0) ? "送料無料" : moneyDualHtml({ jpy: null, usd: v.ebay.shippingUsd, fxRate: v.fxRate, align: "left" }), html: Number.isFinite(v.ebay.shippingUsd) && v.ebay.shippingUsd !== 0 },
    { key: "仕入原価合計", val: moneyDualHtml({ jpy: v.jp.totalJpy, usd: toNumber(v.calc?.jpCostUsd ?? v.jp.totalUsd), fxRate: v.fxRate, align: "left" }), html: true },
    { key: "  └ 日本商品価格", val: moneyDualHtml({ jpy: v.jp.priceJpy, usd: null, fxRate: v.fxRate, align: "left" }), html: true },
    { key: "  └ 日本国内送料", val: (Number.isFinite(v.jp.shippingJpy) && v.jp.shippingJpy === 0) ? "送料無料" : moneyDualHtml({ jpy: v.jp.shippingJpy, usd: null, fxRate: v.fxRate, align: "left" }), html: Number.isFinite(v.jp.shippingJpy) && v.jp.shippingJpy !== 0 },
    { key: "変動手数料(販売+決済)", val: moneyDualHtml({ jpy: null, usd: toNumber(v.calc?.variableFeeUsd), fxRate: v.fxRate, align: "left" }), html: true },
    { key: "総コスト", val: moneyDualHtml({ jpy: null, usd: toNumber(v.calc?.usdCostTotal), fxRate: v.fxRate, align: "left" }), html: true },
    { key: "期待利益", val: moneyDualHtml({ jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate, align: "left" }), html: true },
    { key: "期待粗利率", val: formatPercent(v.expectedMarginRate) },
    { key: "使用為替", val: v.fxRate ? `1 USD = ${v.fxRate.toFixed(4)} JPY` : "-" },
    { key: "為替ソース", val: candidate.fx_source || "-" },
  ];
  if (String(v.ebay.saleBasisType || "").toLowerCase() !== "active_listing_price") {
    priceRows.splice(2, 0, { key: "eBay価格基準", val: saleBasisLabel(v.ebay.saleBasisType) });
  }

  const liquidityRows = [];
  if (v.liquidity) {
    const sold90d = toNumber(v.liquidity.sold_90d_count);
    const soldMinInfo = getSoldMinOutlierInfo(v.liquidity);
    liquidityRows.push({ key: "90日売却件数", val: Number.isFinite(sold90d) && sold90d >= 0 ? `${sold90d}件` : "未取得" });
    if (soldMinInfo.isOutlier) {
      const rawText = moneyDualText({ jpy: null, usd: soldMinInfo.soldMinRaw, fxRate: v.fxRate });
      const ratioText = Number.isFinite(soldMinInfo.ratio) ? ` (ratio=${soldMinInfo.ratio.toFixed(3)})` : "";
      liquidityRows.push({ key: "90日最低成約価格", val: `外れ値除外（raw ${rawText}${ratioText}）` });
    } else if (Number.isFinite(soldMinInfo.soldMin) && soldMinInfo.soldMin > 0) {
      liquidityRows.push({
        key: "90日最低成約価格",
        val: moneyDualHtml({ jpy: null, usd: soldMinInfo.soldMin, fxRate: v.fxRate, align: "left" }),
        html: true,
      });
    } else if (Number.isFinite(soldMinInfo.soldMinRaw) && soldMinInfo.soldMinRaw > 0) {
      liquidityRows.push({
        key: "90日最低成約価格(raw)",
        val: moneyDualHtml({ jpy: null, usd: soldMinInfo.soldMinRaw, fxRate: v.fxRate, align: "left" }),
        html: true,
      });
    }
    liquidityRows.push({ key: "流動性ソース", val: v.liquidity.source || "-" });
    liquidityRows.push({ key: "流動性Gate", val: v.liquidity.gate_passed ? "通過" : `除外(${v.liquidity.gate_reason || "unknown"})` });
  }

  if (v.ev90) {
    liquidityRows.push({
      key: "EV90",
      val: moneyDualHtml({ jpy: null, usd: toNumber(v.ev90.score_usd), fxRate: v.fxRate, align: "left" }),
      html: true,
    });
    liquidityRows.push({
      key: "EV90閾値",
      val: moneyDualHtml({ jpy: null, usd: toNumber(v.ev90.min_required_usd), fxRate: v.fxRate, align: "left" }),
      html: true,
    });
    liquidityRows.push({ key: "90日売却確率", val: formatPercent(toNumber(v.ev90.prob_sell_90d)) });
    liquidityRows.push({
      key: "保管コスト(90日)",
      val: moneyDualHtml({ jpy: null, usd: toNumber(v.ev90.holding_cost_usd), fxRate: v.fxRate, align: "left" }),
      html: true,
    });
    liquidityRows.push({
      key: "リスク控除",
      val: moneyDualHtml({ jpy: null, usd: toNumber(v.ev90.risk_penalty_usd), fxRate: v.fxRate, align: "left" }),
      html: true,
    });
    liquidityRows.push({ key: "EV90判定", val: v.ev90.pass ? "通過" : "除外" });
  }

  const opsRows = [
    { key: "作成日時", val: formatIsoShort(candidate.created_at) },
    { key: "更新日時", val: formatIsoShort(candidate.updated_at) },
    { key: "出品状態(ダミー)", val: candidate.listing_state || "-" },
    { key: "出品参照ID", val: candidate.listing_reference || "-" },
  ];

  const groups = [
    { title: "照合・判定情報", rows: matchRows },
    { title: "価格と利益の算出", rows: priceRows },
    { title: "流動性・EV90", rows: liquidityRows },
    { title: "監査ログ", rows: opsRows },
  ].filter((group) => Array.isArray(group.rows) && group.rows.length > 0);

  refs.calcData.innerHTML = groups.map((group) => {
    const title = escapeHtml(String(group.title || "-"));
    const rowsHtml = group.rows.map((row) => {
      const key = escapeHtml(String(row.key ?? "-"));
      const value = row.html ? String(row.val ?? "-") : escapeHtml(String(row.val ?? "-"));
      return `<li><span class="calc-k">${key}:</span> <span class="calc-v">${value}</span></li>`;
    }).join("");
    return `<section class="calc-group"><h3>${title}</h3><ul class="calc-list">${rowsHtml}</ul></section>`;
  }).join("");
  refs.rawJson.textContent = JSON.stringify(candidate, null, 2);
  scheduleFinanceCellHeightSync();
}

function escapeHtml(str) {
  return str
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function api(path, options = {}) {
  const target = /^https?:\/\//i.test(path)
    ? path
    : (API_BASE ? `${API_BASE}${path.startsWith("/") ? "" : "/"}${path}` : path);
  const res = await fetch(target, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data?.error?.message || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return data;
}

function queueForTab(tab) {
  return tab === "reviewed" ? state.queues.reviewed : state.queues.pending;
}

function isReviewedStatus(status) {
  const key = String(status || "").toLowerCase();
  return key === "approved" || key === "rejected" || key === "listed";
}

function renderTabState() {
  const isPending = state.activeTab === "pending";
  refs.tabPending.classList.toggle("active", isPending);
  refs.tabReviewed.classList.toggle("active", !isPending);
  refs.tabPending.setAttribute("aria-selected", isPending ? "true" : "false");
  refs.tabReviewed.setAttribute("aria-selected", isPending ? "false" : "true");
  const pendingCount = Number.isFinite(Number(state.queueTotals.pending))
    ? Number(state.queueTotals.pending)
    : state.queues.pending.length;
  const reviewedCount = Number.isFinite(Number(state.queueTotals.reviewed))
    ? Number(state.queueTotals.reviewed)
    : state.queues.reviewed.length;
  refs.countPending.textContent = formatTabCount(pendingCount);
  refs.countReviewed.textContent = formatTabCount(reviewedCount);
}

function formatTabCount(value) {
  const n = Math.max(0, Math.floor(Number(value || 0)));
  return n > TAB_COUNT_CAP ? `${TAB_COUNT_CAP}+` : String(n);
}

function pickDominantCandidateIdInViewport() {
  const container = refs.reviewList;
  if (!container) return null;
  const cRect = container.getBoundingClientRect();
  let bestId = null;
  let bestScore = -1;
  const cards = Array.from(container.querySelectorAll("[data-candidate-id]"));
  for (const card of cards) {
    const rect = card.getBoundingClientRect();
    const visibleTop = Math.max(rect.top, cRect.top);
    const visibleBottom = Math.min(rect.bottom, cRect.bottom);
    const visible = Math.max(0, visibleBottom - visibleTop);
    if (visible <= 0) continue;
    const ratio = rect.height > 0 ? visible / rect.height : 0;
    const score = visible * (0.35 + ratio);
    if (score > bestScore) {
      const id = Number(card.dataset.candidateId || "");
      if (Number.isFinite(id)) {
        bestScore = score;
        bestId = id;
      }
    }
  }
  return bestId;
}

function markActiveCandidateInList(candidateId) {
  const id = Number(candidateId || 0);
  refs.reviewList.querySelectorAll("[data-candidate-id]").forEach((el) => {
    const cid = Number(el.getAttribute("data-candidate-id") || 0);
    el.classList.toggle("active", Number.isFinite(id) && id > 0 && cid === id);
  });
}

function mergeCandidateIntoQueues(candidate) {
  if (!candidate || !Number.isFinite(Number(candidate.id))) return;
  const id = Number(candidate.id);
  const replace = (rows) => rows.map((row) => (Number(row.id) === id ? candidate : row));
  state.queues.pending = replace(state.queues.pending);
  state.queues.reviewed = replace(state.queues.reviewed);
}

function candidateNeedsRejectionHydrate(candidate) {
  const status = String(candidate?.status || "").toLowerCase();
  if (status !== "rejected") return false;
  const rejections = Array.isArray(candidate?.rejections) ? candidate.rejections : [];
  return rejections.length === 0;
}

async function hydrateCandidateDetailIfNeeded(candidateId, candidate) {
  if (!candidateNeedsRejectionHydrate(candidate)) return;
  const id = Number(candidateId);
  if (!Number.isFinite(id)) return;
  const seq = ++state.detailFetchSeq;
  try {
    const detail = await api(`/v1/miner/candidates/${id}`);
    if (!detail || Number(detail.id) !== id) return;
    state.detailCache.set(id, detail);
    mergeCandidateIntoQueues(detail);
    if (seq !== state.detailFetchSeq) return;
    if (Number(state.current?.id) === id) {
      state.current = detail;
      markActiveCandidateInList(id);
      renderCandidate(detail);
    }
    if (state.activeTab === "reviewed") {
      renderReviewList();
      markActiveCandidateInList(id);
    }
  } catch (_) {
    // keep lightweight flow even if detail fetch fails
  }
}

async function warmReviewedRejectionDetails(limit = 12) {
  const ids = state.queues.reviewed
    .filter((row) => String(row?.status || "").toLowerCase() === "rejected")
    .map((row) => Number(row.id))
    .filter((id) => Number.isFinite(id) && !state.detailCache.has(id))
    .slice(0, limit);
  if (ids.length === 0) return;
  await Promise.all(ids.map(async (id) => {
    try {
      const detail = await api(`/v1/miner/candidates/${id}`);
      if (!detail || Number(detail.id) !== id) return;
      state.detailCache.set(id, detail);
      mergeCandidateIntoQueues(detail);
    } catch (_) {
      // ignore prefetch failures
    }
  }));
  if (state.activeTab === "reviewed") {
    renderReviewList();
    markActiveCandidateInList(Number(state.current?.id || 0));
  }
}

function scheduleScrollDrivenSelection() {
  if (!refs.reviewList) return;
  if (state.scrollSelectRaf) return;
  state.scrollSelectRaf = window.requestAnimationFrame(async () => {
    state.scrollSelectRaf = null;
    const dominantId = pickDominantCandidateIdInViewport();
    const currentId = Number(state.current?.id || 0);
    if (!Number.isFinite(dominantId) || dominantId === currentId) return;
    state.selectingFromScroll = true;
    try {
      await selectCandidateById(dominantId, { fetchDetail: false, skipListRender: true });
    } finally {
      state.selectingFromScroll = false;
    }
  });
}

function renderReviewList() {
  const items = queueForTab(state.activeTab);
  const currentId = Number(state.current?.id || 0);
  refs.reviewList.innerHTML = items.map((rawItem) => {
    const id = Number(rawItem.id || 0);
    const cached = state.detailCache.get(id);
    const item = (cached && Number(cached.id) === id) ? cached : rawItem;
    const isActive = id === currentId;
    const colorRisk = getColorRiskInfo(item);
    const v = normalize(item);
    const profitText = moneyDualInlineHtml({ jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate });
    const score = toNumber(item.match_score);
    const soldCount = Number.isFinite(v.ebay.soldCount90d) && v.ebay.soldCount90d >= 0 ? `${v.ebay.soldCount90d}件` : "未取得";
    const soldMin = Number.isFinite(v.ebay.soldMin90d) && v.ebay.soldMin90d > 0
      ? moneyDualInlineHtml({ jpy: null, usd: v.ebay.soldMin90d, fxRate: v.fxRate })
      : "未取得";
    const rejection = getLatestRejection(item);
    const rejectionTargets = rejection ? formatIssueTargetsJa(rejection.issue_targets) : "";
    const rejectionReason = rejection ? compactText(rejection.reason_text, 38) : "";
    const ebayTotal = moneyDualInlineHtml({ jpy: null, usd: v.ebay.totalUsd, fxRate: v.fxRate });
    const ebayPrice = moneyDualInlineHtml({ jpy: null, usd: v.ebay.priceUsd, fxRate: v.fxRate });
    const ebayShipping = shippingInlineHtml({ jpy: null, usd: v.ebay.shippingUsd, fxRate: v.fxRate });
    const jpTotal = moneyDualInlineHtml({ jpy: v.jp.totalJpy, usd: v.jp.totalUsd, fxRate: v.fxRate });
    const jpPrice = moneyDualInlineHtml({ jpy: v.jp.priceJpy, usd: null, fxRate: v.fxRate });
    const jpShipping = shippingInlineHtml({ jpy: v.jp.shippingJpy, usd: null, fxRate: v.fxRate });
    const marketExtracted = buildExtractedSnapshot(v.ebay);
    const sourceExtracted = buildExtractedSnapshot(v.jp);
    const marketHref = resolveDisplayLink(v.ebay.itemUrl, v.ebay.site, v.ebay.title, {
      allowFallback: false,
      itemIdHint: v.ebay.itemId,
    }).href;
    const sourceHref = resolveDisplayLink(v.jp.itemUrl, v.jp.site, v.jp.title).href;
    const marketThumbBody = v.ebay.imageUrl
      ? `<img class="pair-thumb" src="${escapeHtml(v.ebay.imageUrl)}" alt="eBay商品画像" />`
      : `<div class="pair-thumb placeholder">NO IMAGE</div>`;
    const sourceThumbBody = v.jp.imageUrl
      ? `<img class="pair-thumb" src="${escapeHtml(v.jp.imageUrl)}" alt="日本商品画像" />`
      : `<div class="pair-thumb placeholder">NO IMAGE</div>`;
    const marketThumb = marketHref
      ? `<a class="pair-thumb-link" href="${escapeHtml(marketHref)}" target="_blank" rel="noopener noreferrer">${marketThumbBody}</a>`
      : marketThumbBody;
    const sourceThumb = sourceHref
      ? `<a class="pair-thumb-link" href="${escapeHtml(sourceHref)}" target="_blank" rel="noopener noreferrer">${sourceThumbBody}</a>`
      : sourceThumbBody;
    const stockLabel = String(v.jp.stockStatus || "").trim();
    const warnChip = colorRisk.hasColorMissingRisk ? '<span class="pair-chip">色要確認</span>' : "";
    const rejectedChip = String(item.status || "").toLowerCase() === "rejected"
      ? `<span class="pair-chip reject">否認: ${escapeHtml(rejectionTargets || "詳細読込中")}${rejectionReason ? ` / ${escapeHtml(rejectionReason)}` : ""}</span>`
      : "";
    return `
      <li>
        <article class="review-item candidate-pair-full ${isActive ? "active" : ""}" data-candidate-id="${id}" tabindex="0">
          <div class="candidate-pair-grid">
            <section class="candidate-side market">
              <div class="candidate-side-head">
                <strong>eBay（販売側）</strong>
                <span>${v.ebay.isSoldItemReference ? "90日売却済み" : `#${id}`}</span>
              </div>
              ${marketThumb}
              <p class="candidate-title">${escapeHtml(v.ebay.title || "-")}</p>
              <div class="candidate-extract">
                <p>メーカー: ${escapeHtml(marketExtracted.brand)}</p>
                <p>型番: ${escapeHtml(marketExtracted.model)}</p>
              </div>
              <div class="candidate-meta">
                <p>状態: ${escapeHtml(marketExtracted.condition)}</p>
                <p>商品: ${ebayPrice}</p>
                <p>送料: ${ebayShipping}</p>
                <p>合計: ${ebayTotal}</p>
                <p>90日売却: ${escapeHtml(soldCount)}</p>
              </div>
            </section>
            <section class="candidate-side source">
              <div class="candidate-side-head">
                <strong>${escapeHtml(labelForSite(item.source_site || "jp"))}（仕入側）</strong>
                ${stockLabel ? `<span>${escapeHtml(stockLabel)}</span>` : ""}
              </div>
              ${sourceThumb}
              <p class="candidate-title">${escapeHtml(v.jp.title || "-")}</p>
              <div class="candidate-extract">
                <p>メーカー: ${escapeHtml(sourceExtracted.brand)}</p>
                <p>型番: ${escapeHtml(sourceExtracted.model)}</p>
              </div>
              <div class="candidate-meta">
                <p>状態: ${escapeHtml(sourceExtracted.condition)}</p>
                <p>商品: ${jpPrice}</p>
                <p>送料: ${jpShipping}</p>
                <p>合計: ${jpTotal}</p>
                ${stockLabel ? `<p>在庫: ${escapeHtml(stockLabel)}</p>` : ""}
              </div>
            </section>
          </div>
          <div class="pair-footer">
            <span class="pair-chip strong">利益 ${profitText}</span>
            <span class="pair-chip">90日売却 ${escapeHtml(soldCount)}</span>
            <span class="pair-chip">90日最低 ${soldMin}</span>
            <span class="pair-chip muted">score ${Number.isFinite(score) ? score.toFixed(3) : "-"}</span>
            ${warnChip}
            ${rejectedChip}
          </div>
        </article>
      </li>
    `;
  }).join("");
  refs.reviewListEmpty.style.display = items.length ? "none" : "block";
  markActiveCandidateInList(currentId);
  scheduleScrollDrivenSelection();
}

async function fetchPendingQueue() {
  const params = new URLSearchParams({ status: "pending", limit: "200" });
  params.set("min_profit_usd", "0.01");
  params.set("min_margin_rate", "0.03");
  params.set("min_match_score", String(DEFAULT_MIN_MATCH_SCORE));
  params.set("condition", "new");
  const data = await api(`/v1/miner/queue?${params.toString()}`);
  return {
    items: Array.isArray(data.items) ? data.items : [],
    total: Number(data.total || 0),
  };
}

async function fetchReviewedQueue() {
  const params = new URLSearchParams({ status: "reviewed", limit: "200" });
  const data = await api(`/v1/miner/queue?${params.toString()}`);
  const items = Array.isArray(data.items) ? data.items : [];
  return {
    items: items.filter((item) => isReviewedStatus(item.status)),
    total: Number(data.total || 0),
  };
}

function firstCandidateId(items) {
  if (!Array.isArray(items) || items.length === 0) return null;
  const id = Number(items[0].id);
  return Number.isFinite(id) ? id : null;
}

function findCandidateInQueues(candidateId) {
  const id = Number(candidateId);
  if (!Number.isFinite(id)) return null;
  const inPending = state.queues.pending.some((item) => Number(item.id) === id);
  if (inPending) return { tab: "pending", id };
  const inReviewed = state.queues.reviewed.some((item) => Number(item.id) === id);
  if (inReviewed) return { tab: "reviewed", id };
  return null;
}

function findCandidateInMemory(candidateId) {
  const id = Number(candidateId);
  if (!Number.isFinite(id)) return null;
  for (const row of state.queues.pending) {
    if (Number(row.id) === id) return row;
  }
  for (const row of state.queues.reviewed) {
    if (Number(row.id) === id) return row;
  }
  return null;
}

async function selectCandidateById(candidateId, options = {}) {
  const fetchDetail = Boolean(options.fetchDetail);
  const skipListRender = Boolean(options.skipListRender);
  const id = Number(candidateId);
  if (!Number.isFinite(id)) return;
  const cached = state.detailCache.get(id);
  const inMemory = findCandidateInMemory(id);
  let selected = (cached && Number(cached.id) === id) ? cached : inMemory;
  if (!selected || fetchDetail) {
    try {
      const detail = await api(`/v1/miner/candidates/${id}`);
      if (detail && Number(detail.id) === id) {
        selected = detail;
        state.detailCache.set(id, detail);
        mergeCandidateIntoQueues(detail);
      }
    } catch (_) {
      if (!selected) throw _;
    }
  }
  state.current = selected || null;
  if (skipListRender) {
    markActiveCandidateInList(id);
  } else {
    renderReviewList();
  }
  renderCandidate(state.current);
  if (state.current) {
    void hydrateCandidateDetailIfNeeded(id, state.current);
  }
}

async function refreshQueues({ preserveSelection = true } = {}) {
  const prevId = preserveSelection ? Number(state.current?.id || 0) : null;
  const pendingPayload = await fetchPendingQueue();
  const reviewedPayload = await fetchReviewedQueue();
  state.queues.pending = pendingPayload.items;
  state.queues.reviewed = reviewedPayload.items;
  state.queueTotals.pending = Math.max(0, Number(pendingPayload.total || 0));
  state.queueTotals.reviewed = Math.max(0, Number(reviewedPayload.total || 0));
  void warmReviewedRejectionDetails();

  renderTabState();

  let next = findCandidateInQueues(prevId);
  if (!next) {
    const activeItems = queueForTab(state.activeTab);
    const activeId = firstCandidateId(activeItems);
    if (Number.isFinite(activeId)) {
      next = { tab: state.activeTab, id: activeId };
    }
  }
  if (!next) {
    const otherTab = state.activeTab === "pending" ? "reviewed" : "pending";
    const otherId = firstCandidateId(queueForTab(otherTab));
    if (Number.isFinite(otherId)) {
      next = { tab: otherTab, id: otherId };
      state.activeTab = otherTab;
      renderTabState();
    }
  }

  renderReviewList();

  if (!next) {
    state.current = null;
    renderCandidate(null);
    return;
  }
  await selectCandidateById(next.id, { skipListRender: true });
}

async function setActiveTab(tab) {
  const nextTab = tab === "reviewed" ? "reviewed" : "pending";
  if (state.activeTab === nextTab) {
    renderReviewList();
    return;
  }
  state.activeTab = nextTab;
  renderTabState();
  renderReviewList();
  if (nextTab === "reviewed") {
    void warmReviewedRejectionDetails();
  }

  const activeItems = queueForTab(nextTab);
  const currentId = Number(state.current?.id || 0);
  const existsCurrent = activeItems.some((item) => Number(item.id) === currentId);
  if (existsCurrent) return;
  const firstId = firstCandidateId(activeItems);
  if (Number.isFinite(firstId)) {
    await selectCandidateById(firstId, { skipListRender: true });
    return;
  }
  state.current = null;
  renderCandidate(null);
}

function getCheckedIssueTargets() {
  return Array.from(refs.issueTargets.querySelectorAll('input[type="checkbox"]:checked')).map((el) => el.value);
}

async function onApprove() {
  if (!state.current) return;
  const id = state.current.id;
  const wasAutoApproved = String(state.current.status || "").toLowerCase() === "approved";
  await api(`/v1/miner/candidates/${id}/approve`, { method: "POST" });
  showToast(
    wasAutoApproved
      ? `候補 #${id} を最終承認し、ダミー出品へ遷移しました。`
      : `候補 #${id} を承認し、ダミー出品へ遷移しました。`
  );
  await refreshQueues({ preserveSelection: false });
}

async function onReject() {
  if (!state.current) return;
  const id = state.current.id;
  const wasAutoApproved = String(state.current.status || "").toLowerCase() === "approved";
  const issueTargets = getCheckedIssueTargets();
  const reason = refs.reasonText.value.trim();
  if (!issueTargets.length) {
    throw new Error("指摘箇所を1つ以上選択してください。");
  }
  await api(`/v1/miner/candidates/${id}/reject`, {
    method: "POST",
    body: JSON.stringify({ issue_targets: issueTargets, reason_text: reason }),
  });
  refs.reasonText.value = "";
  refs.issueTargets.querySelectorAll('input[type="checkbox"]').forEach((el) => {
    el.checked = false;
  });
  showToast(
    wasAutoApproved
      ? `候補 #${id}（自動承認済み）を否認として保存しました。`
      : `候補 #${id} を否認として保存しました。`
  );
  await refreshQueues({ preserveSelection: false });
}

function mountIssueTargets() {
  refs.issueTargets.innerHTML = ISSUE_TARGET_OPTIONS.map(
    ([key, label]) =>
      `<label class="issue-cell" data-issue-key="${escapeHtml(key)}"><input type="checkbox" value="${escapeHtml(key)}" /> <span>${escapeHtml(label)}</span></label>`
  ).join("");
}

function populateCategoryOptions(options) {
  const normalized = Array.isArray(options) && options.length
    ? options
    : FALLBACK_CATEGORIES;
  refs.fetchQuery.innerHTML = normalized.map((row) => {
    const value = String(row.value || "").trim();
    const label = String(row.label || row.value || "").trim();
    if (!value) return "";
    return `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`;
  }).join("");
  if (!refs.fetchQuery.value && normalized[0]?.value) {
    refs.fetchQuery.value = String(normalized[0].value);
  }
}

async function loadCategoryOptions() {
  try {
    const data = await api("/v1/miner/category-options");
    const rows = Array.isArray(data?.items) ? data.items : [];
    const options = rows
      .map((row) => {
        if (!row || typeof row !== "object") return null;
        const value = String(row.value || row.category_key || "").trim();
        const label = String(row.label || row.display_name_ja || value).trim();
        if (!value) return null;
        return { value, label };
      })
      .filter(Boolean);
    populateCategoryOptions(options);
  } catch (_) {
    populateCategoryOptions(FALLBACK_CATEGORIES);
  }
}

async function onFetchLiveCandidates() {
  const query = refs.fetchQuery.value.trim();
  if (!query) {
    throw new Error("カテゴリを選択してください。");
  }
  const cfg = buildFetchConfig();

  refs.fetchBtn.disabled = true;
  refs.fetchBtn.textContent = "探索中... 0%";
  startRpaProgressPolling();
  try {
    const payload = await api("/v1/miner/fetch", {
      method: "POST",
      body: JSON.stringify({
        query,
        source_sites: LIVE_FETCH_SOURCE_SITES,
        market_site: "ebay",
        timed_mode: true,
        target_min_candidates: 3,
        fetch_timebox_sec: 60,
        fetch_max_passes: 4,
        continue_after_target: true,
        require_in_stock: cfg.requireInStock,
        limit_per_site: cfg.limitPerSite,
        max_candidates: cfg.maxCandidates,
        min_match_score: cfg.minMatchScore,
        min_profit_usd: cfg.minProfitUsd,
        min_margin_rate: cfg.minMarginRate,
      }),
    });

    state.lastFetch = payload;
    await pollRpaProgressOnce();
    state.optimisticProgress = Math.max(state.displayedProgress, 96);
    state.displayedProgress = Math.max(state.displayedProgress, 96);
    renderRpaProgress(
      {
        status: "running",
        phase: "timed_fetch_finalize",
        message: "探索結果を画面へ反映中",
        progress_percent: state.displayedProgress,
        updated_ago_sec: 0,
      },
      { running: true }
    );
    stopRpaProgressPolling();
    renderFetchStats(payload);

    state.activeTab = "pending";
    renderTabState();
    await refreshQueues({ preserveSelection: false });

    const createdIds = Array.isArray(payload.created_ids) ? payload.created_ids : [];
    if (createdIds.length > 0) {
      await selectCandidateById(createdIds[0]);
    }

    state.optimisticProgress = 100;
    state.displayedProgress = 100;
    renderRpaProgress(
      {
        status: "completed",
        phase: "completed",
        message: "探索処理が完了しました",
        progress_percent: 100,
        updated_ago_sec: 0,
      },
      { running: false }
    );

    const createdCount = Number(payload.created_count || 0);
    const duplicateCount = Number(payload.skipped_duplicates || 0);
    const unprofitableCount = Number(payload.skipped_unprofitable || 0);
    const lowMarginCount = Number(payload.skipped_low_margin || 0);
    const missingSoldMinCount = Number(payload.skipped_missing_sold_min || 0);
    const nonMinBasisCount = Number(payload.skipped_non_min_basis || 0);
    const missingSoldSampleCount = Number(payload.skipped_missing_sold_sample || 0);
    const belowSoldMinCount = Number(payload.skipped_below_sold_min || 0);
    const implausibleSoldMinCount = Number(payload.skipped_implausible_sold_min || 0);
    const unresolvedVariantPriceCount = Number(payload.skipped_source_variant_unresolved || 0);
    const lowLiquidityCount = Number(payload.skipped_low_liquidity || 0);
    const liquidityUnavailableCount = Number(payload.skipped_liquidity_unavailable || 0);
    const queryCacheSkip = Boolean(payload.query_cache_skip);
    const queryCacheTtlSec = Number(payload.query_cache_ttl_sec || 0);
    const hints = Array.isArray(payload.hints) ? payload.hints : [];
    const timedFetch = (payload && typeof payload.timed_fetch === "object") ? payload.timed_fetch : null;
    const timedPasses = Number(timedFetch?.passes_run || 0);
    const stage1PassTotal = Number(timedFetch?.stage1_pass_total || 0);
    const stage2Runs = Number(timedFetch?.stage2_runs || 0);
    const timedReason = String(timedFetch?.stop_reason || "").trim();
    const stage1SkipCounts = (payload.stage1_skip_counts && typeof payload.stage1_skip_counts === "object")
      ? payload.stage1_skip_counts
      : {};
    const stage1Top = Object.entries(stage1SkipCounts)
      .map(([k, v]) => [String(k), Number(v)])
      .filter(([, v]) => Number.isFinite(v) && v > 0)
      .sort((a, b) => b[1] - a[1])[0];
    const dailyLimitReached = isRpaDailyLimitReached(payload);

    let msg = `探索完了: ${createdCount}件追加`;
    if (timedPasses > 1) msg += ` / ${timedPasses}パス`;
    if (stage1PassTotal > 0 || stage2Runs > 0) msg += ` / 一次通過${stage1PassTotal}件 / 最終再判定${stage2Runs}件`;
    if (duplicateCount > 0) msg += ` / 重複${duplicateCount}件スキップ`;
    if (unprofitableCount > 0) msg += ` / 低利益${unprofitableCount}件除外`;
    if (lowMarginCount > 0) msg += ` / 低粗利率${lowMarginCount}件除外`;
    if (missingSoldMinCount > 0) msg += ` / 90日最低未取得${missingSoldMinCount}件除外`;
    if (nonMinBasisCount > 0) msg += ` / 90日最低基準外${nonMinBasisCount}件除外`;
    if (missingSoldSampleCount > 0) msg += ` / 売却済み参照欠損${missingSoldSampleCount}件除外`;
    if (belowSoldMinCount > 0) msg += ` / 仕入>=90日最低${belowSoldMinCount}件除外`;
    if (implausibleSoldMinCount > 0) msg += ` / 90日最低異常値${implausibleSoldMinCount}件除外`;
    if (unresolvedVariantPriceCount > 0) msg += ` / 型番別価格未特定${unresolvedVariantPriceCount}件除外`;
    if (lowLiquidityCount > 0) msg += ` / 低流動性${lowLiquidityCount}件除外`;
    if (liquidityUnavailableCount > 0) msg += ` / 流動性未取得${liquidityUnavailableCount}件除外`;
    if (queryCacheSkip) {
      msg += queryCacheTtlSec > 0
        ? ` / 同条件スキップ中(${queryCacheTtlSec}秒)`
        : " / 同条件スキップ中";
    }
    if (dailyLimitReached) msg += " / Product Research上限到達";
    if (timedReason) msg += ` / 停止理由:${timedReason}`;
    if (stage1Top) msg += ` / 一次トップ除外:${skipReasonLabel(stage1Top[0])}${Number(stage1Top[1])}件`;
    if (hints.length > 0) msg += ` / ${hints[0]}`;
    showToast(msg);
  } catch (err) {
    state.optimisticProgress = 100;
    state.displayedProgress = 100;
    renderRpaProgress(
      {
        status: "failed",
        phase: "failed",
        message: `探索失敗: ${err instanceof Error ? err.message : "unknown"}`,
        progress_percent: 100,
        updated_ago_sec: 0,
      },
      { running: false }
    );
    throw err;
  } finally {
    stopRpaProgressPolling();
    refs.fetchBtn.disabled = false;
    refs.fetchBtn.textContent = "探索開始";
  }
}

function bindEvents() {
  refs.fetchQuery.addEventListener("change", () => {
    void refreshSeedPoolStatusForCurrentCategory({ updateHeadline: true });
  });

  refs.fetchBtn.addEventListener("click", async () => {
    try {
      await onFetchLiveCandidates();
    } catch (err) {
      showToast(`取得エラー: ${err.message}`);
    }
  });

  refs.openSettingsBtn?.addEventListener("click", () => {
    openSettingsOverlay();
  });

  refs.closeSettingsBtn?.addEventListener("click", () => {
    closeSettingsOverlay();
  });

  refs.settingsOverlay?.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.closeSettings === "1") {
      closeSettingsOverlay();
    }
  });

  window.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (refs.settingsOverlay && !refs.settingsOverlay.hidden) {
      closeSettingsOverlay();
    }
  });

  window.addEventListener("resize", () => {
    scheduleFinanceCellHeightSync();
  });

  refs.reloadBtn.addEventListener("click", async () => {
    try {
      await refreshQueues({ preserveSelection: true });
      showToast("候補一覧を更新しました。");
    } catch (err) {
      showToast(`再読込エラー: ${err.message}`);
    }
  });

  refs.tabPending.addEventListener("click", async () => {
    try {
      await setActiveTab("pending");
    } catch (err) {
      showToast(`タブ切替エラー: ${err.message}`);
    }
  });

  refs.tabReviewed.addEventListener("click", async () => {
    try {
      await setActiveTab("reviewed");
    } catch (err) {
      showToast(`タブ切替エラー: ${err.message}`);
    }
  });

  refs.reviewList.addEventListener("scroll", () => {
    if (!state.selectingFromScroll) {
      scheduleScrollDrivenSelection();
    }
  }, { passive: true });

  refs.reviewList.addEventListener("click", async (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const card = target.closest("[data-candidate-id]");
    if (!card) return;
    const id = Number(card.dataset.candidateId || "");
    if (!Number.isFinite(id)) return;
    try {
      await selectCandidateById(id, { fetchDetail: true, skipListRender: true });
    } catch (err) {
      showToast(`候補取得エラー: ${err.message}`);
    }
  });

  refs.reviewList.addEventListener("keydown", async (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const target = event.target;
    if (!(target instanceof Element)) return;
    const card = target.closest("[data-candidate-id]");
    if (!card) return;
    event.preventDefault();
    const id = Number(card.dataset.candidateId || "");
    if (!Number.isFinite(id)) return;
    try {
      await selectCandidateById(id, { fetchDetail: true, skipListRender: true });
    } catch (err) {
      showToast(`候補取得エラー: ${err.message}`);
    }
  });

  refs.approveBtn.addEventListener("click", async () => {
    try {
      await onApprove();
    } catch (err) {
      showToast(`承認エラー: ${err.message}`);
    }
  });

  refs.rejectBtn.addEventListener("click", async () => {
    try {
      await onReject();
    } catch (err) {
      showToast(`否認エラー: ${err.message}`);
    }
  });
}

async function init() {
  mountIssueTargets();
  bindEvents();
  renderFetchStats(state.lastFetch);
  if (refs.endpointLabel) {
    const endpointLabel = API_BASE || window.location.origin;
    refs.endpointLabel.textContent = `API接続先: ${endpointLabel}`;
  }
  await loadCategoryOptions();
  await refreshSeedPoolStatusForCurrentCategory({ updateHeadline: true });
  try {
    await refreshQueues({ preserveSelection: false });
    scheduleFinanceCellHeightSync();
    window.setTimeout(() => scheduleFinanceCellHeightSync(), 180);
    showToast("Miner画面を更新しました。");
  } catch (err) {
    showToast(`初期化エラー: ${err.message}`);
  }
}

init();
