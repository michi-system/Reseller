const ISSUE_TARGET_OPTIONS = [
  ["brand", "ブランド"],
  ["model", "型番"],
  ["color", "色"],
  ["size", "サイズ"],
  ["accessories", "付属"],
  ["condition", "状態"],
  ["price", "価格"],
  ["shipping", "送料"],
  ["fees", "手数料"],
  ["fx", "為替"],
  ["other", "その他"],
];
const ISSUE_TARGET_LABEL_MAP = Object.fromEntries(ISSUE_TARGET_OPTIONS);

const LIVE_FETCH_SOURCE_SITES = ["rakuten", "yahoo"];
const DEFAULT_MIN_MATCH_SCORE = 0.75;

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
  sumLiquidityActiveStr: document.getElementById("sumLiquidityActiveStr"),
  sumLiquidityGate: document.getElementById("sumLiquidityGate"),
  sumFeeRates: document.getElementById("sumFeeRates"),
  sumOtherCosts: document.getElementById("sumOtherCosts"),
  decisionTone: document.getElementById("decisionTone"),
  decisionLabel: document.getElementById("decisionLabel"),
  decisionSub: document.getElementById("decisionSub"),
  decisionReasons: document.getElementById("decisionReasons"),
  riskFlags: document.getElementById("riskFlags"),
  fetchStatusHeadline: document.getElementById("fetchStatusHeadline"),
  fetchStatsRows: document.getElementById("fetchStatsRows"),
  calcDigest: document.getElementById("calcDigest"),
  calcData: document.getElementById("calcData"),
  rawJson: document.getElementById("rawJson"),
};

const state = {
  queues: {
    pending: [],
    reviewed: [],
  },
  activeTab: "pending",
  current: null,
  lastFetch: null,
  detailCache: new Map(),
  detailFetchSeq: 0,
  scrollSelectRaf: null,
  selectingFromScroll: false,
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

function resolveApiBase() {
  const params = new URLSearchParams(window.location.search || "");
  const fromQuery = (params.get("apiBase") || "").trim();
  if (fromQuery) {
    try {
      const url = new URL(fromQuery, window.location.origin);
      const normalized = `${url.protocol}//${url.host}`;
      window.localStorage.setItem("review_api_base", normalized);
      return normalized;
    } catch (_) {
      // ignore invalid apiBase param
    }
  }

  const stored = (window.localStorage.getItem("review_api_base") || "").trim();
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

function moneyDualHtml({ jpy, usd, fxRate, align = "right" }) {
  const pair = moneyPair({ jpy, usd, fxRate });
  if (!Number.isFinite(pair.jpy) && !Number.isFinite(pair.usd)) {
    return "-";
  }
  const main = Number.isFinite(pair.jpy) ? formatJpy(pair.jpy) : "-";
  const sub = Number.isFinite(pair.usd) ? formatUsd(pair.usd) : "-";
  return `<span class="money-stack ${align === "left" ? "left" : "right"}"><span class="money-main">${escapeHtml(main)}</span><small class="money-sub">${escapeHtml(sub)}</small></span>`;
}

function setMoneyCell(el, { jpy, usd, fxRate, align = "right" }) {
  if (!el) return;
  el.innerHTML = moneyDualHtml({ jpy, usd, fxRate, align });
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

function shippingText({ jpy, usd, fxRate }) {
  if ((Number.isFinite(usd) && usd === 0) || (Number.isFinite(jpy) && jpy === 0)) {
    return "送料無料";
  }
  return moneyDualText({ jpy, usd, fxRate });
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
  const matches = upper.match(/[A-Z0-9][A-Z0-9-]{3,}/g) || [];
  const out = [];
  const seen = new Set();
  for (const token of matches) {
    const norm = token.replace(/^-+|-+$/g, "");
    if (!norm) continue;
    if (MODEL_CODE_STOPWORDS.has(norm)) continue;
    if (norm.length < 4) continue;
    if (!/[0-9]/.test(norm)) continue;
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
  if (reason === "error") return "APIエラー発生のため停止";
  return "";
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
      reasons.push({ text: `最終利益 ${moneyDualText({ jpy: normalized?.expectedProfitJpy, usd: profitUsd, fxRate: normalized?.fxRate })}`, tone: "good" });
    } else {
      reasons.push({ text: `最終利益が赤字 ${moneyDualText({ jpy: normalized?.expectedProfitJpy, usd: profitUsd, fxRate: normalized?.fxRate })}`, tone: "warn" });
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
  const autoReview = (meta && typeof meta.auto_review === "object" && meta.auto_review) ? meta.auto_review : null;
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
  const jpPrice = pickNumber(meta, ["jp_price_jpy", "source_price_jpy", "purchase_price_jpy"], null);
  const jpShipping = pickNumber(meta, ["jp_shipping_jpy", "source_shipping_jpy", "domestic_shipping_jpy"], 0);

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

  return {
    meta,
    fxRate,
    ebay: {
      title: (hasSoldItemReference && soldItemTitle) ? soldItemTitle : (candidate.market_title || "-"),
      site: candidate.market_site || "ebay",
      itemId: String(candidate.market_item_id || "").trim(),
      imageUrl: (hasSoldItemReference && soldImageUrl) ? soldImageUrl : pickImage(meta, "ebay"),
      itemUrl: (hasSoldItemReference && soldItemUrl)
        ? soldItemUrl
        : pick(meta, ["ebay_item_url", "market_item_url", "market_url"], null),
      condition: String(pick(meta, ["market_condition"], candidate.condition || "") || ""),
      identifiers: marketIdentifiers,
      priceUsd: ebayPrice,
      shippingUsd: ebayShipping,
      totalUsd: ebayPrice === null ? null : ebayTotal,
      soldCount90d,
      soldMin90d,
      saleBasisType,
      isSoldItemReference: hasSoldItemReference,
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

function renderFetchStats(payload) {
  if (!refs.fetchStatusHeadline || !refs.fetchStatsRows) return;
  if (!payload || typeof payload !== "object") {
    refs.fetchStatusHeadline.textContent = "まだ探索を実行していません。";
    refs.fetchStatsRows.innerHTML = "";
    return;
  }

  const createdCount = Number(payload.created_count || 0);
  const searchScopeDone = Boolean(payload.search_scope_done);
  const queryCacheSkip = Boolean(payload.query_cache_skip);
  const queryCacheTtlSec = Number(payload.query_cache_ttl_sec || 0);
  const errors = Array.isArray(payload.errors) ? payload.errors : [];
  const appliedFilters = (payload.applied_filters && typeof payload.applied_filters === "object")
    ? payload.applied_filters
    : {};
  const fetched = (payload.fetched && typeof payload.fetched === "object") ? payload.fetched : {};

  let headline = `今回の探索: 追加 ${createdCount}件`;
  if (searchScopeDone) headline += " / 探索完走";
  if (queryCacheSkip) {
    headline += queryCacheTtlSec > 0
      ? ` / 同条件スキップ中(${queryCacheTtlSec}秒)`
      : " / 同条件スキップ中";
  }
  headline += Boolean(appliedFilters.require_in_stock) ? " / 在庫あり必須" : " / 在庫条件なし";
  if (errors.length > 0) headline += ` / エラー${errors.length}件`;
  refs.fetchStatusHeadline.textContent = headline;

  const rows = Object.entries(fetched);
  if (rows.length === 0) {
    refs.fetchStatsRows.innerHTML = `
      <div class="fetch-row">
        <div class="head"><span>探索ログなし</span><span class="fetch-stop">-</span></div>
        <p class="fetch-note">この探索ではサイト別の取得データがありませんでした。</p>
      </div>
    `;
    return;
  }

  refs.fetchStatsRows.innerHTML = rows.map(([site, info]) => {
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
    const m = path.match(/\/itm\/(?:[^/]+\/)?(\d+)/);
    if (!m) return false;
    const itemId = m[1] || "";
    return itemId.length !== 12;
  } catch {
    return true;
  }
}

function resolveDisplayLink(url, site, title) {
  const raw = String(url || "").trim();
  if (!raw) {
    return { href: buildSearchUrl(site, title), fallbackUsed: true };
  }
  try {
    const u = new URL(raw);
    if (!/^https?:$/.test(u.protocol)) {
      return { href: buildSearchUrl(site, title), fallbackUsed: true };
    }
    if (String(site || "").toLowerCase() === "ebay" && isLikelyInvalidEbayItemUrl(raw)) {
      return { href: buildSearchUrl(site, title), fallbackUsed: true };
    }
    return { href: raw, fallbackUsed: false };
  } catch {
    return { href: buildSearchUrl(site, title), fallbackUsed: true };
  }
}

function renderLink(aEl, url, site, title) {
  const resolved = resolveDisplayLink(url, site, title);
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
    if (refs.sumLiquidityActiveStr) refs.sumLiquidityActiveStr.textContent = "-";
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
    return;
  }

  const v = normalize(candidate);
  const status = String(candidate.status || "").toLowerCase();
  const autoReview = (v.meta && typeof v.meta.auto_review === "object") ? v.meta.auto_review : null;
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

  const ebayHref = renderLink(refs.ebayLink, v.ebay.itemUrl, v.ebay.site, v.ebay.title);
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
    const base = `商品 ${moneyDualText({ jpy: null, usd: v.ebay.priceUsd, fxRate: v.fxRate })} / 送料 ${shippingText({ jpy: null, usd: v.ebay.shippingUsd, fxRate: v.fxRate })}`;
    const hasSpecialBasis = String(v.ebay.saleBasisType || "").toLowerCase() !== "active_listing_price";
    const note = hasSpecialBasis ? ` / ${saleBasisLabel(v.ebay.saleBasisType)}` : "";
    setOptionalNote(refs.sumRevenueBreakdown, `${base}${note}`);
  }
  setMoneyCell(refs.sumPurchase, { jpy: v.jp.totalJpy, usd: purchaseUsd, fxRate: v.fxRate });
  if (refs.sumPurchaseBreakdown) {
    setOptionalNote(
      refs.sumPurchaseBreakdown,
      `商品 ${moneyDualText({ jpy: v.jp.priceJpy, usd: null, fxRate: v.fxRate })} / 国内送料 ${shippingText({ jpy: v.jp.shippingJpy, usd: null, fxRate: v.fxRate })}`
    );
  }
  setMoneyCell(refs.sumExpenses, { jpy: null, usd: expenseUsd, fxRate: v.fxRate });
  if (refs.sumExpensesBreakdown) {
    const pieces = [];
    if (Number.isFinite(v.calc?.variableFeeUsd)) pieces.push(`変動手数料 ${formatUsd(v.calc.variableFeeUsd)}`);
    if (Number.isFinite(v.calc?.intlShippingUsd)) pieces.push(`国際送料 ${formatUsd(v.calc.intlShippingUsd)}`);
    if (Number.isFinite(v.calc?.customsUsd)) pieces.push(`関税 ${formatUsd(v.calc.customsUsd)}`);
    if (Number.isFinite(v.calc?.packagingUsd)) pieces.push(`梱包 ${formatUsd(v.calc.packagingUsd)}`);
    if (Number.isFinite(v.calc?.fixedFeeUsd)) pieces.push(`固定費 ${formatUsd(v.calc.fixedFeeUsd)}`);
    if (Number.isFinite(v.calc?.miscCostUsd)) pieces.push(`その他 ${formatUsd(v.calc.miscCostUsd)}`);
    if (pieces.length === 0) setOptionalNote(refs.sumExpensesBreakdown, "", "未取得");
    else if (pieces.length <= 2) setOptionalNote(refs.sumExpensesBreakdown, pieces.join(" / "));
    else setOptionalNote(refs.sumExpensesBreakdown, `${pieces[0]} / ${pieces[1]} ほか${pieces.length - 2}件`);
  }
  setMoneyCell(refs.sumProfit, { jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate });
  if (refs.sumProfitBreakdown) {
    const marginText = Number.isFinite(v.expectedMarginRate) ? `粗利率 ${formatPercent(v.expectedMarginRate)}` : "";
    const grossText = Number.isFinite(grossDiffUsd) ? `粗差額 ${moneyDualText({ jpy: null, usd: grossDiffUsd, fxRate: v.fxRate })}` : "";
    const rows = [marginText, grossText].filter(Boolean);
    setOptionalNote(refs.sumProfitBreakdown, rows.join(" / "));
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
  const liqActive = toNumber(v.liquidity?.active_count);
  const liqStr = toNumber(v.liquidity?.sell_through_90d);
  if (refs.sumLiquidityActiveStr) {
    const activeText = Number.isFinite(liqActive) && liqActive >= 0 ? `${liqActive}件` : "未取得";
    const strText = Number.isFinite(liqStr) && liqStr >= 0 ? formatPercent(liqStr) : "未取得";
    refs.sumLiquidityActiveStr.textContent = `${activeText} / ${strText}`;
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
      ? decisionReasons.map((row) => `<span class="reason-chip ${row.tone === "warn" ? "warn" : "good"}">${escapeHtml(row.text)}</span>`).join("")
      : '<span class="reason-chip">判定理由なし</span>';
  }

  if (refs.calcDigest) {
    const scoreText = toNumber(candidate.match_score)?.toFixed(3) ?? "-";
    const soldText = Number.isFinite(v.ebay.soldCount90d) && v.ebay.soldCount90d >= 0 ? `${v.ebay.soldCount90d}件` : "未取得";
    const soldMinText = Number.isFinite(v.ebay.soldMin90d) && v.ebay.soldMin90d > 0
      ? moneyDualText({ jpy: null, usd: v.ebay.soldMin90d, fxRate: v.fxRate })
      : "未取得";
    const profitText = moneyDualHtml({ jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate, align: "left" });
    const grossDiffText = moneyDualText({ jpy: null, usd: grossDiffUsd, fxRate: v.fxRate });
    const variableFeeText = moneyDualText({ jpy: null, usd: toNumber(v.calc?.variableFeeUsd), fxRate: v.fxRate });
    const profitEquation = Number.isFinite(revenueUsd) && Number.isFinite(costTotalUsd)
      ? `${formatUsd(revenueUsd)} - ${formatUsd(costTotalUsd)}`
      : "-";
    refs.calcDigest.innerHTML = `
      <span class="digest-row">
        <span class="digest-chip"><strong>一致スコア</strong>${escapeHtml(scoreText)}</span>
        <span class="digest-chip"><strong>90日売却</strong>${escapeHtml(soldText)}</span>
        <span class="digest-chip"><strong>90日最低</strong>${escapeHtml(soldMinText)}</span>
        <span class="digest-chip"><strong>粗差額</strong>${escapeHtml(grossDiffText)}</span>
        <span class="digest-chip"><strong>変動手数料</strong>${escapeHtml(variableFeeText)}</span>
        <span class="digest-chip"><strong>利益式</strong>${escapeHtml(profitEquation)}</span>
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
    const activeCount = toNumber(v.liquidity.active_count);
    const sellThrough = toNumber(v.liquidity.sell_through_90d);
    const soldMinInfo = getSoldMinOutlierInfo(v.liquidity);
    liquidityRows.push({ key: "90日売却件数", val: Number.isFinite(sold90d) && sold90d >= 0 ? `${sold90d}件` : "未取得" });
    liquidityRows.push({ key: "アクティブ件数", val: Number.isFinite(activeCount) && activeCount >= 0 ? `${activeCount}件` : "未取得" });
    liquidityRows.push({ key: "90日STR", val: Number.isFinite(sellThrough) && sellThrough >= 0 ? formatPercent(sellThrough) : "未取得" });
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
  refs.countPending.textContent = String(state.queues.pending.length);
  refs.countReviewed.textContent = String(state.queues.reviewed.length);
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
    const detail = await api(`/v1/review/candidates/${id}`);
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
      const detail = await api(`/v1/review/candidates/${id}`);
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
    const profitText = moneyDualText({ jpy: v.expectedProfitJpy, usd: v.expectedProfitUsd, fxRate: v.fxRate });
    const score = toNumber(item.match_score);
    const soldCount = Number.isFinite(v.ebay.soldCount90d) && v.ebay.soldCount90d >= 0 ? `${v.ebay.soldCount90d}件` : "未取得";
    const soldMin = Number.isFinite(v.ebay.soldMin90d) && v.ebay.soldMin90d > 0
      ? moneyDualText({ jpy: null, usd: v.ebay.soldMin90d, fxRate: v.fxRate })
      : "未取得";
    const rejection = getLatestRejection(item);
    const rejectionTargets = rejection ? formatIssueTargetsJa(rejection.issue_targets) : "";
    const rejectionReason = rejection ? compactText(rejection.reason_text, 38) : "";
    const ebayTotal = moneyDualText({ jpy: null, usd: v.ebay.totalUsd, fxRate: v.fxRate });
    const ebayPrice = moneyDualText({ jpy: null, usd: v.ebay.priceUsd, fxRate: v.fxRate });
    const ebayShipping = shippingText({ jpy: null, usd: v.ebay.shippingUsd, fxRate: v.fxRate });
    const jpTotal = moneyDualText({ jpy: v.jp.totalJpy, usd: v.jp.totalUsd, fxRate: v.fxRate });
    const jpPrice = moneyDualText({ jpy: v.jp.priceJpy, usd: null, fxRate: v.fxRate });
    const jpShipping = shippingText({ jpy: v.jp.shippingJpy, usd: null, fxRate: v.fxRate });
    const marketExtracted = buildExtractedSnapshot(v.ebay);
    const sourceExtracted = buildExtractedSnapshot(v.jp);
    const marketHref = resolveDisplayLink(v.ebay.itemUrl, v.ebay.site, v.ebay.title).href;
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
                <p>商品: ${escapeHtml(ebayPrice)}</p>
                <p>送料: ${escapeHtml(ebayShipping)}</p>
                <p>合計: ${escapeHtml(ebayTotal)}</p>
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
                <p>商品: ${escapeHtml(jpPrice)}</p>
                <p>送料: ${escapeHtml(jpShipping)}</p>
                <p>合計: ${escapeHtml(jpTotal)}</p>
                ${stockLabel ? `<p>在庫: ${escapeHtml(stockLabel)}</p>` : ""}
              </div>
            </section>
          </div>
          <div class="pair-footer">
            <span class="pair-chip strong">利益 ${escapeHtml(profitText)}</span>
            <span class="pair-chip">90日売却 ${escapeHtml(soldCount)}</span>
            <span class="pair-chip">90日最低 ${escapeHtml(soldMin)}</span>
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
  const data = await api(`/v1/review/queue?${params.toString()}`);
  return Array.isArray(data.items) ? data.items : [];
}

async function fetchReviewedQueue() {
  const params = new URLSearchParams({ status: "all", limit: "200" });
  const data = await api(`/v1/review/queue?${params.toString()}`);
  const allItems = Array.isArray(data.items) ? data.items : [];
  return allItems.filter((item) => isReviewedStatus(item.status));
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
      const detail = await api(`/v1/review/candidates/${id}`);
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
  const [pending, reviewed] = await Promise.all([fetchPendingQueue(), fetchReviewedQueue()]);
  state.queues.pending = pending;
  state.queues.reviewed = reviewed;
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
  await api(`/v1/review/candidates/${id}/approve`, { method: "POST" });
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
  await api(`/v1/review/candidates/${id}/reject`, {
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
    const data = await api("/v1/review/category-options");
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
  refs.fetchBtn.textContent = "探索中...";
  try {
    const payload = await api("/v1/review/fetch", {
      method: "POST",
      body: JSON.stringify({
        query,
        source_sites: LIVE_FETCH_SOURCE_SITES,
        market_site: "ebay",
        require_in_stock: cfg.requireInStock,
        limit_per_site: cfg.limitPerSite,
        max_candidates: cfg.maxCandidates,
        min_match_score: cfg.minMatchScore,
        min_profit_usd: cfg.minProfitUsd,
        min_margin_rate: cfg.minMarginRate,
      }),
    });

    state.lastFetch = payload;
    renderFetchStats(payload);

    state.activeTab = "pending";
    renderTabState();
    await refreshQueues({ preserveSelection: false });

    const createdIds = Array.isArray(payload.created_ids) ? payload.created_ids : [];
    if (createdIds.length > 0) {
      await selectCandidateById(createdIds[0]);
    }

    const createdCount = Number(payload.created_count || 0);
    const duplicateCount = Number(payload.skipped_duplicates || 0);
    const unprofitableCount = Number(payload.skipped_unprofitable || 0);
    const lowMarginCount = Number(payload.skipped_low_margin || 0);
    const missingSoldMinCount = Number(payload.skipped_missing_sold_min || 0);
    const missingSoldSampleCount = Number(payload.skipped_missing_sold_sample || 0);
    const belowSoldMinCount = Number(payload.skipped_below_sold_min || 0);
    const implausibleSoldMinCount = Number(payload.skipped_implausible_sold_min || 0);
    const lowLiquidityCount = Number(payload.skipped_low_liquidity || 0);
    const liquidityUnavailableCount = Number(payload.skipped_liquidity_unavailable || 0);
    const queryCacheSkip = Boolean(payload.query_cache_skip);
    const queryCacheTtlSec = Number(payload.query_cache_ttl_sec || 0);
    const hints = Array.isArray(payload.hints) ? payload.hints : [];

    let msg = `探索完了: ${createdCount}件追加`;
    if (duplicateCount > 0) msg += ` / 重複${duplicateCount}件スキップ`;
    if (unprofitableCount > 0) msg += ` / 低利益${unprofitableCount}件除外`;
    if (lowMarginCount > 0) msg += ` / 低粗利率${lowMarginCount}件除外`;
    if (missingSoldMinCount > 0) msg += ` / 90日最低未取得${missingSoldMinCount}件除外`;
    if (missingSoldSampleCount > 0) msg += ` / 売却済み参照欠損${missingSoldSampleCount}件除外`;
    if (belowSoldMinCount > 0) msg += ` / 仕入>=90日最低${belowSoldMinCount}件除外`;
    if (implausibleSoldMinCount > 0) msg += ` / 90日最低異常値${implausibleSoldMinCount}件除外`;
    if (lowLiquidityCount > 0) msg += ` / 低流動性${lowLiquidityCount}件除外`;
    if (liquidityUnavailableCount > 0) msg += ` / 流動性未取得${liquidityUnavailableCount}件除外`;
    if (queryCacheSkip) {
      msg += queryCacheTtlSec > 0
        ? ` / 同条件スキップ中(${queryCacheTtlSec}秒)`
        : " / 同条件スキップ中";
    }
    if (hints.length > 0) msg += ` / ${hints[0]}`;
    showToast(msg);
  } finally {
    refs.fetchBtn.disabled = false;
    refs.fetchBtn.textContent = "探索開始";
  }
}

function bindEvents() {
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
  try {
    await refreshQueues({ preserveSelection: false });
    showToast("レビュー画面を更新しました。");
  } catch (err) {
    showToast(`初期化エラー: ${err.message}`);
  }
}

init();
