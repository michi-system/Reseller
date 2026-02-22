const refs = {
  endpointLabel: document.getElementById("endpointLabel"),
  refreshAllBtn: document.getElementById("refreshAllBtn"),
  sumTotal: document.getElementById("sumTotal"),
  sumReady: document.getElementById("sumReady"),
  sumListed: document.getElementById("sumListed"),
  sumAlert: document.getElementById("sumAlert"),
  sumStopped: document.getElementById("sumStopped"),
  ingestPath: document.getElementById("ingestPath"),
  ingestBtn: document.getElementById("ingestBtn"),
  listingLimit: document.getElementById("listingLimit"),
  listingDryRun: document.getElementById("listingDryRun"),
  actorId: document.getElementById("actorId"),
  listingCycleBtn: document.getElementById("listingCycleBtn"),
  monitorLimit: document.getElementById("monitorLimit"),
  obsPath: document.getElementById("obsPath"),
  monitorLightBtn: document.getElementById("monitorLightBtn"),
  monitorHeavyBtn: document.getElementById("monitorHeavyBtn"),
  cfgMinProfitJpy: document.getElementById("cfgMinProfitJpy"),
  cfgMinProfitRate: document.getElementById("cfgMinProfitRate"),
  cfgStopStreak: document.getElementById("cfgStopStreak"),
  cfgLightNew: document.getElementById("cfgLightNew"),
  cfgLightStable: document.getElementById("cfgLightStable"),
  cfgLightStopped: document.getElementById("cfgLightStopped"),
  cfgHeavyDays: document.getElementById("cfgHeavyDays"),
  saveConfigBtn: document.getElementById("saveConfigBtn"),
  jobList: document.getElementById("jobList"),
  jobEmpty: document.getElementById("jobEmpty"),
  stateFilter: document.getElementById("stateFilter"),
  listLimit: document.getElementById("listLimit"),
  reloadListBtn: document.getElementById("reloadListBtn"),
  listingBody: document.getElementById("listingBody"),
  listingEmpty: document.getElementById("listingEmpty"),
  detailTitle: document.getElementById("detailTitle"),
  detailMetrics: document.getElementById("detailMetrics"),
  manualNote: document.getElementById("manualNote"),
  manualAlertBtn: document.getElementById("manualAlertBtn"),
  manualStopBtn: document.getElementById("manualStopBtn"),
  manualKeepListedBtn: document.getElementById("manualKeepListedBtn"),
  manualResumeReadyBtn: document.getElementById("manualResumeReadyBtn"),
  eventList: document.getElementById("eventList"),
  eventEmpty: document.getElementById("eventEmpty"),
  snapshotList: document.getElementById("snapshotList"),
  snapshotEmpty: document.getElementById("snapshotEmpty"),
  toast: document.getElementById("toast"),
};

const state = {
  listings: [],
  selectedId: null,
  selectedListing: null,
};

const STATE_LABELS = {
  ready: "準備中 (ready)",
  listed: "出品中 (listed)",
  alert_review: "要確認 (alert_review)",
  stopped: "停止中 (stopped)",
};

const JOB_TYPE_LABELS = {
  ingest_approved_jsonl: "承認データ取込",
  listing_cycle: "出品サイクル",
  monitor_cycle_light: "軽量監視サイクル",
  monitor_cycle_heavy: "重量監視サイクル",
};

const JOB_STATUS_LABELS = {
  success: "成功",
  partial_success: "一部成功",
  failed: "失敗",
  running: "実行中",
};

const CHECK_TYPE_LABELS = {
  light: "軽量監視",
  heavy: "重量監視",
};

const DECISION_LABELS = {
  keep: "維持",
  stop: "停止",
  alert_review: "要確認",
};

const EVENT_TYPE_LABELS = {
  listed_dry_run: "試運転出品",
  listed_live: "本番出品",
  auto_stop: "自動停止",
  alert_review: "要確認化",
  back_to_listed: "出品中へ復帰",
  restart_candidate: "再開候補検知",
  manual_stop: "手動停止",
  manual_alert_review: "手動で要確認化",
  manual_keep_listed: "手動で出品継続",
  manual_resume_ready: "手動で準備中へ戻し",
};

const REASON_LABELS = {
  low_profit: "利益不足",
  low_stock: "在庫不足",
  heavy_price_drop: "価格急落",
  restart_candidate_detected: "再開候補検知",
  manual_stop: "手動停止",
  manual_alert_review: "手動で要確認化",
  manual_keep_listed: "手動で出品継続",
  manual_resume_ready: "手動で準備中へ戻し",
};

function labelState(value) {
  const key = String(value || "").trim().toLowerCase();
  return STATE_LABELS[key] || key || "-";
}

function labelJobType(value) {
  const key = String(value || "").trim().toLowerCase();
  return JOB_TYPE_LABELS[key] || key || "-";
}

function labelJobStatus(value) {
  const key = String(value || "").trim().toLowerCase();
  return JOB_STATUS_LABELS[key] || key || "-";
}

function labelCheckType(value) {
  const key = String(value || "").trim().toLowerCase();
  return CHECK_TYPE_LABELS[key] || key || "-";
}

function labelDecision(value) {
  const key = String(value || "").trim().toLowerCase();
  return DECISION_LABELS[key] || key || "-";
}

function labelEventType(value) {
  const key = String(value || "").trim().toLowerCase();
  const label = EVENT_TYPE_LABELS[key];
  if (label) return `${label} (${key})`;
  return key || "-";
}

function labelReason(value) {
  const key = String(value || "").trim().toLowerCase();
  const label = REASON_LABELS[key];
  if (label) return `${label} (${key})`;
  return key || "-";
}

function toInt(value, fallback = 0) {
  const num = Number(value);
  if (!Number.isFinite(num)) return fallback;
  return Math.trunc(num);
}

function toFloat(value, fallback = 0) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatJpy(value) {
  if (!Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("ja-JP", { style: "currency", currency: "JPY", maximumFractionDigits: 0 }).format(value);
}

function formatUsd(value) {
  if (!Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("ja-JP", { style: "currency", currency: "USD", maximumFractionDigits: 2 }).format(value);
}

function formatPercent(rate) {
  if (!Number.isFinite(rate)) return "-";
  return `${(rate * 100).toFixed(1)}%`;
}

function formatDate(text) {
  const raw = String(text || "").trim();
  if (!raw) return "-";
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return raw;
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const da = String(d.getDate()).padStart(2, "0");
  const h = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${y}-${mo}-${da} ${h}:${mi}`;
}

function showToast(text) {
  if (!refs.toast) return;
  refs.toast.textContent = String(text || "");
  refs.toast.classList.add("show");
  window.setTimeout(() => refs.toast.classList.remove("show"), 2200);
}

function resolveApiBase() {
  const params = new URLSearchParams(window.location.search || "");
  const fromQuery = (params.get("apiBase") || "").trim();
  if (fromQuery) {
    try {
      const url = new URL(fromQuery, window.location.origin);
      const normalized = `${url.protocol}//${url.host}`;
      window.localStorage.setItem("operator_api_base", normalized);
      return normalized;
    } catch (_) {
      // ignore
    }
  }

  const stored = (window.localStorage.getItem("operator_api_base") || "").trim();
  if (stored) return stored.replace(/\/+$/, "");

  const host = window.location.hostname;
  const isLocalHost = host === "127.0.0.1" || host === "localhost";
  if (window.location.protocol === "file:") return "http://127.0.0.1:8012";
  if (isLocalHost && window.location.port !== "8012") return "http://127.0.0.1:8012";
  if (window.location.port === "8012") return "";
  return "";
}

const API_BASE = resolveApiBase();

async function api(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await response.text();
  let payload = {};
  if (text.trim()) {
    try {
      payload = JSON.parse(text);
    } catch (_) {
      payload = { raw: text };
    }
  }
  if (!response.ok) {
    const msg = payload?.error?.message || `HTTP ${response.status}`;
    throw new Error(msg);
  }
  return payload;
}

function setBusy(button, busy) {
  if (!button) return;
  button.disabled = Boolean(busy);
  button.dataset.busy = busy ? "1" : "0";
}

function stateToneClass(stateName) {
  const s = String(stateName || "").toLowerCase();
  if (s === "listed") return "tone-listed";
  if (s === "ready") return "tone-ready";
  if (s === "alert_review") return "tone-alert";
  if (s === "stopped") return "tone-stopped";
  return "";
}

function renderJobs(items) {
  const rows = Array.isArray(items) ? items : [];
  refs.jobList.innerHTML = rows
    .slice(0, 12)
    .map((row) => {
      const jobType = escapeHtml(labelJobType(row.job_type));
      const status = escapeHtml(labelJobStatus(row.status));
      const started = escapeHtml(formatDate(row.started_at));
      const finished = escapeHtml(formatDate(row.finished_at));
      const processed = toInt(row.processed_count, 0);
      const ok = toInt(row.success_count, 0);
      const ng = toInt(row.error_count, 0);
      return `<li><strong>${jobType}</strong> / ${status}<br>${started} -> ${finished}<br>処理:${processed} 成功:${ok} 失敗:${ng}</li>`;
    })
    .join("");
  refs.jobEmpty.hidden = rows.length > 0;
}

function renderSummary(summary) {
  const counts = summary?.listing_counts || {};
  refs.sumReady.textContent = String(toInt(counts.ready_count, 0));
  refs.sumListed.textContent = String(toInt(counts.listed_count, 0));
  refs.sumAlert.textContent = String(toInt(counts.alert_review_count, 0));
  refs.sumStopped.textContent = String(toInt(counts.stopped_count, 0));
  refs.sumTotal.textContent = String(toInt(counts.total_count, 0));
  renderJobs(summary?.latest_jobs || []);
}

function renderListings() {
  const rows = Array.isArray(state.listings) ? state.listings : [];
  refs.listingBody.innerHTML = rows
    .map((row) => {
      const id = toInt(row.id, 0);
      const active = id === state.selectedId ? "active" : "";
      const tone = stateToneClass(row.listing_state);
      const profit = formatJpy(toFloat(row.current_profit_jpy, NaN));
      const margin = formatPercent(toFloat(row.current_profit_rate, NaN));
      return `<tr data-id="${id}" class="${active} ${tone}">
        <td>${id}</td>
        <td>${escapeHtml(labelState(row.listing_state))}</td>
        <td>${escapeHtml(row.sku_key || "-")}</td>
        <td>${escapeHtml(profit)}</td>
        <td>${escapeHtml(margin)}</td>
        <td>${escapeHtml(formatDate(row.next_light_check_at))}</td>
        <td>${escapeHtml(formatDate(row.updated_at))}</td>
      </tr>`;
    })
    .join("");
  refs.listingEmpty.hidden = rows.length > 0;
  refs.listingBody.querySelectorAll("tr[data-id]").forEach((tr) => {
    tr.addEventListener("click", () => {
      const id = toInt(tr.dataset.id, 0);
      if (id > 0) {
        state.selectedId = id;
        void reloadSelectedListing();
      }
    });
  });
}

function metric(label, value) {
  return `<div class="metric"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function renderListingDetail(listing) {
  if (!listing) {
    refs.detailTitle.textContent = "一覧から1件選択してね";
    refs.detailMetrics.innerHTML = "";
    return;
  }
  refs.detailTitle.textContent = `#${toInt(listing.id, 0)} ${listing.title || "(タイトルなし)"}`;
  refs.detailMetrics.innerHTML = [
    metric("状態", labelState(listing.listing_state)),
    metric("承認ID", listing.approved_id || "-"),
    metric("SKU", listing.sku_key || "-"),
    metric("出品ID", listing.channel_listing_id || "-"),
    metric("仕入れ市場", listing.source_market || "-"),
    metric("販売市場", listing.target_market || "-"),
    metric("現在仕入れ価格", formatJpy(toFloat(listing.current_source_price_jpy, NaN))),
    metric("現在販売価格", formatUsd(toFloat(listing.current_target_price_usd, NaN))),
    metric("現利益(JPY)", formatJpy(toFloat(listing.current_profit_jpy, NaN))),
    metric("現利益率", formatPercent(toFloat(listing.current_profit_rate, NaN))),
    metric("仕入れ在庫", toInt(listing.source_in_stock, 0) === 1 ? "あり" : "なし"),
    metric("要確認フラグ", String(toInt(listing.needs_review, 0))),
    metric("次回軽量監視", formatDate(listing.next_light_check_at)),
    metric("次回重量監視", formatDate(listing.next_heavy_check_at)),
    metric("更新日時", formatDate(listing.updated_at)),
    metric("作成日時", formatDate(listing.created_at)),
  ].join("");
}

function renderEvents(items) {
  const rows = Array.isArray(items) ? items : [];
  refs.eventList.innerHTML = rows
    .map((row) => {
      const type = escapeHtml(labelEventType(row.event_type));
      const reason = escapeHtml(labelReason(row.reason_code));
      const actor = escapeHtml(row.actor_id || "-");
      const note = escapeHtml(row.note || "");
      const at = escapeHtml(formatDate(row.created_at));
      return `<li><strong>${type}</strong> / ${reason}<br>作業者:${actor} 実行:${at}<br>${note}</li>`;
    })
    .join("");
  refs.eventEmpty.hidden = rows.length > 0;
}

function renderSnapshots(items) {
  const rows = Array.isArray(items) ? items : [];
  refs.snapshotList.innerHTML = rows
    .slice(0, 20)
    .map((row) => {
      const kind = escapeHtml(labelCheckType(row.check_type));
      const decision = escapeHtml(labelDecision(row.decision));
      const reason = escapeHtml(labelReason(row.reason_code));
      const at = escapeHtml(formatDate(row.captured_at));
      const profit = escapeHtml(formatJpy(toFloat(row.profit_jpy, NaN)));
      const rate = escapeHtml(formatPercent(toFloat(row.profit_rate, NaN)));
      return `<li><strong>${kind}</strong> ${decision} / ${reason}<br>利益:${profit} 利益率:${rate}<br>${at}</li>`;
    })
    .join("");
  refs.snapshotEmpty.hidden = rows.length > 0;
}

function setConfigInputs(cfg) {
  refs.cfgMinProfitJpy.value = String(toFloat(cfg.min_profit_jpy, 1500));
  refs.cfgMinProfitRate.value = String(toFloat(cfg.min_profit_rate, 0.08));
  refs.cfgStopStreak.value = String(toInt(cfg.stop_consecutive_fail_count, 2));
  refs.cfgLightNew.value = String(toInt(cfg.light_interval_new_hours, 6));
  refs.cfgLightStable.value = String(toInt(cfg.light_interval_stable_hours, 24));
  refs.cfgLightStopped.value = String(toInt(cfg.light_interval_stopped_hours, 72));
  refs.cfgHeavyDays.value = String(toInt(cfg.heavy_interval_days, 7));
}

async function loadSummary() {
  const payload = await api("/v1/operator/summary");
  renderSummary(payload);
  if (refs.endpointLabel) {
    refs.endpointLabel.textContent = `API接続先: ${API_BASE || window.location.origin} / DBパス: ${payload.db_path || "-"}`;
  }
}

async function loadConfig() {
  const payload = await api("/v1/operator/config");
  setConfigInputs(payload.active_config || {});
}

async function loadListings() {
  const stateFilter = String(refs.stateFilter.value || "").trim();
  const limit = Math.max(1, toInt(refs.listLimit.value, 100));
  const qs = new URLSearchParams();
  if (stateFilter) qs.set("state", stateFilter);
  qs.set("limit", String(limit));
  const payload = await api(`/v1/operator/listings?${qs.toString()}`);
  state.listings = Array.isArray(payload.items) ? payload.items : [];
  renderListings();

  if (state.selectedId && !state.listings.some((row) => toInt(row.id, 0) === state.selectedId)) {
    state.selectedId = null;
    state.selectedListing = null;
    renderListingDetail(null);
    renderEvents([]);
    renderSnapshots([]);
  }
}

async function loadEvents(listingId) {
  const payload = await api(`/v1/operator/events?listing_id=${listingId}&limit=100`);
  renderEvents(payload.items || []);
}

async function loadSnapshots(listingId) {
  const payload = await api(`/v1/operator/snapshots?listing_id=${listingId}&limit=100`);
  renderSnapshots(payload.items || []);
}

async function reloadSelectedListing() {
  if (!state.selectedId) {
    renderListingDetail(null);
    renderEvents([]);
    renderSnapshots([]);
    renderListings();
    return;
  }
  const row = await api(`/v1/operator/listings/${state.selectedId}`);
  state.selectedListing = row;
  renderListingDetail(row);
  renderListings();
  await Promise.all([loadEvents(state.selectedId), loadSnapshots(state.selectedId)]);
}

async function refreshAll() {
  await Promise.all([loadSummary(), loadListings(), loadConfig()]);
  if (state.selectedId) {
    await reloadSelectedListing();
  }
}

async function runIngest() {
  setBusy(refs.ingestBtn, true);
  try {
    const payload = await api("/v1/operator/ingest", {
      method: "POST",
      body: JSON.stringify({ input_path: String(refs.ingestPath.value || "").trim() }),
    });
    showToast(`取込完了: +${toInt(payload.inserted_listing_count, 0)}件`);
    await refreshAll();
  } finally {
    setBusy(refs.ingestBtn, false);
  }
}

async function runListingCycle() {
  setBusy(refs.listingCycleBtn, true);
  try {
    const payload = await api("/v1/operator/listing-cycle", {
      method: "POST",
      body: JSON.stringify({
        limit: Math.max(1, toInt(refs.listingLimit.value, 20)),
        dry_run: Boolean(refs.listingDryRun.checked),
        actor_id: String(refs.actorId.value || "").trim(),
      }),
    });
    showToast(`出品サイクル完了: ${toInt(payload.listed_count, 0)}件`);
    await refreshAll();
  } finally {
    setBusy(refs.listingCycleBtn, false);
  }
}

async function runMonitor(checkType, buttonRef) {
  setBusy(buttonRef, true);
  try {
    const obsPath = String(refs.obsPath.value || "").trim();
    const body = {
      check_type: checkType,
      limit: Math.max(1, toInt(refs.monitorLimit.value, 300)),
      actor_id: String(refs.actorId.value || "").trim(),
    };
    if (obsPath) body.observation_jsonl_path = obsPath;
    const payload = await api("/v1/operator/monitor-cycle", {
      method: "POST",
      body: JSON.stringify(body),
    });
    showToast(
      `${labelCheckType(checkType)}完了: 停止 ${toInt(payload.stop_count, 0)}件 / 要確認 ${toInt(payload.alert_count, 0)}件`
    );
    await refreshAll();
  } finally {
    setBusy(buttonRef, false);
  }
}

function buildManualPayload(defaultReason) {
  return {
    actor_id: String(refs.actorId.value || "").trim(),
    reason_code: defaultReason,
    note: String(refs.manualNote.value || "").trim(),
  };
}

async function runManualAction(pathSuffix, defaultReason, buttonRef) {
  if (!state.selectedId) {
    showToast("先に一覧から対象を選んでね");
    return;
  }
  setBusy(buttonRef, true);
  try {
    const payload = await api(`/v1/operator/listings/${state.selectedId}/${pathSuffix}`, {
      method: "POST",
      body: JSON.stringify(buildManualPayload(defaultReason)),
    });
    const nextState = labelState(payload?.action?.next_state || "-");
    showToast(`手動操作完了: ${nextState}`);
    await refreshAll();
    await reloadSelectedListing();
  } finally {
    setBusy(buttonRef, false);
  }
}

async function saveConfig() {
  setBusy(refs.saveConfigBtn, true);
  try {
    const payload = await api("/v1/operator/config", {
      method: "POST",
      body: JSON.stringify({
        created_by: String(refs.actorId.value || "").trim() || "michi-system",
        min_profit_jpy: toFloat(refs.cfgMinProfitJpy.value, 1500),
        min_profit_rate: toFloat(refs.cfgMinProfitRate.value, 0.08),
        stop_consecutive_fail_count: Math.max(1, toInt(refs.cfgStopStreak.value, 2)),
        light_interval_new_hours: Math.max(1, toInt(refs.cfgLightNew.value, 6)),
        light_interval_stable_hours: Math.max(1, toInt(refs.cfgLightStable.value, 24)),
        light_interval_stopped_hours: Math.max(1, toInt(refs.cfgLightStopped.value, 72)),
        heavy_interval_days: Math.max(1, toInt(refs.cfgHeavyDays.value, 7)),
      }),
    });
    showToast(`設定更新: ${payload?.active_config?.config_version || "new"}`);
    await refreshAll();
  } finally {
    setBusy(refs.saveConfigBtn, false);
  }
}

function bindEvents() {
  refs.refreshAllBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await refreshAll();
        showToast("更新したよ");
      } catch (err) {
        showToast(String(err.message || err));
      }
    })();
  });

  refs.reloadListBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await loadListings();
      } catch (err) {
        showToast(String(err.message || err));
      }
    })();
  });

  refs.stateFilter.addEventListener("change", () => {
    void (async () => {
      try {
        await loadListings();
      } catch (err) {
        showToast(String(err.message || err));
      }
    })();
  });

  refs.ingestBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await runIngest();
      } catch (err) {
        showToast(`取込失敗: ${String(err.message || err)}`);
      }
    })();
  });

  refs.listingCycleBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await runListingCycle();
      } catch (err) {
        showToast(`出品失敗: ${String(err.message || err)}`);
      }
    })();
  });

  refs.monitorLightBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await runMonitor("light", refs.monitorLightBtn);
      } catch (err) {
        showToast(`監視失敗: ${String(err.message || err)}`);
      }
    })();
  });

  refs.monitorHeavyBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await runMonitor("heavy", refs.monitorHeavyBtn);
      } catch (err) {
        showToast(`監視失敗: ${String(err.message || err)}`);
      }
    })();
  });

  refs.saveConfigBtn.addEventListener("click", () => {
    void (async () => {
      try {
        await saveConfig();
      } catch (err) {
        showToast(`設定保存失敗: ${String(err.message || err)}`);
      }
    })();
  });

  refs.manualAlertBtn.addEventListener("click", () => {
    void runManualAction("manual-alert", "manual_alert_review", refs.manualAlertBtn).catch((err) => {
      showToast(`手動操作失敗: ${String(err.message || err)}`);
    });
  });

  refs.manualStopBtn.addEventListener("click", () => {
    void runManualAction("manual-stop", "manual_stop", refs.manualStopBtn).catch((err) => {
      showToast(`手動操作失敗: ${String(err.message || err)}`);
    });
  });

  refs.manualKeepListedBtn.addEventListener("click", () => {
    void runManualAction("manual-keep-listed", "manual_keep_listed", refs.manualKeepListedBtn).catch((err) => {
      showToast(`手動操作失敗: ${String(err.message || err)}`);
    });
  });

  refs.manualResumeReadyBtn.addEventListener("click", () => {
    void runManualAction("manual-resume-ready", "manual_resume_ready", refs.manualResumeReadyBtn).catch((err) => {
      showToast(`手動操作失敗: ${String(err.message || err)}`);
    });
  });
}

async function bootstrap() {
  bindEvents();
  try {
    await refreshAll();
  } catch (err) {
    showToast(`初期化失敗: ${String(err.message || err)}`);
  }
}

void bootstrap();
