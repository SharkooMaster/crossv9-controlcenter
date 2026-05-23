"use strict";

// ── Helpers ────────────────────────────────────────────────────────────
const $ = (id) => document.getElementById(id);

function fmtBytes(n) {
  if (n == null || isNaN(n)) return "—";
  const u = ["B","KB","MB","GB","TB","PB"];
  let i = 0; let v = Number(n);
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return v.toFixed(v >= 100 || i === 0 ? 0 : v >= 10 ? 1 : 2) + " " + u[i];
}
function fmtRatio(r) {
  if (!isFinite(r) || r == null) return "—";
  if (r === 0) return "—";
  return (r * 100).toFixed(1) + "%";
}
function fmtMs(v) {
  if (v == null || isNaN(v)) return "—";
  if (v < 1000) return v.toFixed(1) + " ms";
  return (v / 1000).toFixed(2) + " s";
}
function fmtNum(n) {
  if (n == null) return "—";
  return Number(n).toLocaleString("en-US");
}
function fmtTs(ns) {
  if (!ns) return "—";
  const ms = Number(ns) / 1e6;
  const d = new Date(ms);
  const pad = (x) => String(x).padStart(2, "0");
  return pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes()) + ":" + pad(d.getUTCSeconds());
}
function shortJob(j) { return j ? j.substring(0, 8) : ""; }

// ── KPI snapshot ───────────────────────────────────────────────────────
async function refreshSnapshot() {
  try {
    const r = await fetch("/api/snapshot");
    if (!r.ok) return;
    const s = await r.json();
    $("kpi_started").textContent = fmtNum(s.kpis.started);
    $("kpi_completed").textContent = fmtNum(s.kpis.completed);
    $("kpi_failed").textContent = fmtNum(s.kpis.failed);
    $("kpi_active").textContent = fmtNum(s.kpis.active_jobs);
    $("kpi_bytes_in").textContent = fmtBytes(s.kpis.bytes_in);
    $("kpi_bytes_out").textContent = fmtBytes(s.kpis.bytes_out);
    $("kpi_bytes_dc").textContent = fmtBytes(s.kpis.bytes_dc);
    $("kpi_cratio").textContent = fmtRatio(s.kpis.compress_ratio);
    $("kpi_dratio").textContent = fmtRatio(s.kpis.dc_ratio);
    $("kpi_dedup").textContent = fmtRatio(s.kpis.dedup_ratio);
    $("kpi_refs").textContent = fmtNum(s.kpis.refs_found);
    $("kpi_avgms").textContent = fmtMs(s.kpis.avg_server_ms);

    // ── Integrity panel ──
    const checkedEl = $("kpi_int_checked");
    const mismEl = $("kpi_int_mismatch");
    const decEl = $("kpi_int_decompress");
    const rtEl = $("kpi_rt_mismatch");
    checkedEl.textContent = fmtNum(s.kpis.integrity_checked);
    mismEl.textContent = fmtNum(s.kpis.integrity_mismatches);
    decEl.textContent = fmtNum(s.kpis.integrity_decompress_failures);
    // RefRoundTrip combines hash-mismatch + fetch-failure into a single
    // "would decompress fail?" answer. Even one of either is enough to break
    // the file, so display them concatenated like "12 + 3 fetch-fail".
    const rtMism = s.kpis.refroundtrip_mismatches || 0;
    const rtFetchFail = s.kpis.refroundtrip_fetch_failures || 0;
    const rtChecked = s.kpis.refroundtrip_checked || 0;
    if (rtEl) {
      rtEl.textContent = rtFetchFail > 0
        ? `${fmtNum(rtMism)} (${fmtNum(rtFetchFail)} fetch-fail)`
        : fmtNum(rtMism);
      rtEl.classList.toggle("err", rtMism > 0);
      rtEl.classList.toggle("ok", rtMism === 0 && rtChecked > 0);
    }
    mismEl.classList.toggle("err", (s.kpis.integrity_mismatches || 0) > 0);
    mismEl.classList.toggle("ok", (s.kpis.integrity_mismatches || 0) === 0 && (s.kpis.integrity_checked || 0) > 0);
    decEl.classList.toggle("err", (s.kpis.integrity_decompress_failures || 0) > 0);
    decEl.classList.toggle("ok", (s.kpis.integrity_decompress_failures || 0) === 0);
    // Prefer the RefRoundTrip detail over the generic integrity detail when
    // both are present — RefRoundTrip is the only check that catches the
    // "(BucketId, BucketKey) resolves to different bytes than search saw" bug.
    const rtLastDetail = s.kpis.refroundtrip_last_detail;
    const rtLastTs = s.kpis.refroundtrip_last_ts_ns;
    const lastDetail = rtLastDetail || s.kpis.integrity_last_detail;
    const lastTs = rtLastTs || s.kpis.integrity_last_ts_ns;
    const lastEl = $("integrity_last");
    if (lastDetail && lastTs) {
      const label = rtLastDetail ? `RefRoundTrip ${rtLastDetail}` : lastDetail;
      lastEl.textContent = `${fmtTs(lastTs)} · ${label}`;
    } else {
      lastEl.textContent = "—";
    }

    $("subs").textContent = s.bus.sse_subscribers;
    $("journalBytes").textContent = fmtBytes(s.journal.bytes_written);
    $("journalDrop").textContent = fmtNum(s.journal.dropped);

    renderActive(s.active_jobs || []);
  } catch (e) { /* ignore transient errors */ }
}

function renderActive(jobs) {
  const root = $("activeJobs");
  const empty = $("activeEmpty");
  if (!jobs.length) {
    root.innerHTML = "";
    empty.classList.remove("hidden");
    return;
  }
  empty.classList.add("hidden");

  const html = jobs.map((j) => {
    const pct = j.block_count > 0 ? Math.min(100, Math.round((j.blocks_done / j.block_count) * 100)) : 0;
    const ratio = j.bytes_in > 0 ? (j.bytes_out / j.bytes_in * 100).toFixed(1) + "%" : "—";
    return `
      <div class="job" data-job="${j.job_id}">
        <div>
          <div class="name" title="${(j.file_name || "").replace(/"/g,"&quot;")}">${j.file_name || "(no name)"}</div>
          <div class="meta">${j.mode} · ${fmtBytes(j.original_size)} · <span class="pod">${j.cross_pod}</span></div>
        </div>
        <div>
          <div>${j.block_count > 0 ? `${j.blocks_done} / ${j.block_count} blocks` : "monolithic"}</div>
          <div class="meta">in ${fmtBytes(j.bytes_in)} → out ${fmtBytes(j.bytes_out)} (${ratio})</div>
        </div>
        <div>
          <div>${fmtNum(j.refs_found)} refs · ${fmtNum(j.chunks)} chunks</div>
          <div class="meta">dc ${fmtBytes(j.dc_bytes)}</div>
        </div>
        <div class="stage">${j.last_stage || ""}</div>
        <div class="progress"><div class="bar" style="width:${pct}%"></div></div>
      </div>
    `;
  }).join("");
  root.innerHTML = html;
}

// ── Fleet runtime stats ────────────────────────────────────────────────
async function refreshFleet() {
  try {
    const r = await fetch("/api/fleet");
    if (!r.ok) return;
    const fleet = await r.json();
    const body = $("fleetBody");
    const empty = $("fleetEmpty");
    if (!fleet.length) {
      body.innerHTML = "";
      empty.style.display = "";
      return;
    }
    empty.style.display = "none";

    body.innerHTML = fleet.map((f) => {
      const stale = (f.age_sec || 0) > 90;
      const fragPct = (f.heap_fragment_ratio || 0) * 100;
      const fragClass = fragPct >= 50 ? "err" : fragPct >= 30 ? "warn" : "num";
      const ageClass = stale ? "err" : "num";
      const errBadge = f.error ? `<span class="stale-tag" title="${f.error}">${f.error.substring(0,28)}</span>` : "";
      const podLabel = f.pod ? f.pod.split("-").slice(-2).join("-") : (f.ip || "?");
      return `
        <tr>
          <td><span style="color:var(--accent-2)">${f.component}</span></td>
          <td><span class="pod-name" title="${f.pod || ""}">${podLabel}</span>${errBadge}</td>
          <td class="dim">${f.node || ""}</td>
          <td class="num">${fmtBytes(f.heap_size_bytes)}</td>
          <td class="num ${fragClass}">${fragPct.toFixed(1)}%</td>
          <td class="num">${fmtBytes(f.loh_size)}</td>
          <td class="num">${fmtBytes(f.rss_bytes)}</td>
          <td class="num">${fmtBytes(f.native_overhead_bytes)}</td>
          <td class="num">${fmtNum(f.gen2_collections)}</td>
          <td class="num ${ageClass}">${Math.round(f.age_sec || 0)}s</td>
        </tr>
      `;
    }).join("");
  } catch (e) { /* ignore */ }
}

// ── Files list ─────────────────────────────────────────────────────────
async function refreshFiles() {
  try {
    const r = await fetch("/api/files");
    if (!r.ok) return;
    const files = await r.json();
    const tbody = $("filesBody");
    if (!files.length) {
      tbody.innerHTML = `<tr><td colspan="4" style="text-align:center;color:var(--fg-dim);padding:24px">no journal files yet</td></tr>`;
      return;
    }
    tbody.innerHTML = files.map((f) => `
      <tr>
        <td>${f.name}</td>
        <td>${fmtBytes(f.size_bytes)}</td>
        <td>${new Date(f.last_write_utc).toISOString().replace("T"," ").substring(0, 19)}</td>
        <td><a href="/api/files/${encodeURIComponent(f.name)}" download>download</a></td>
      </tr>
    `).join("");
  } catch (e) { /* ignore */ }
}

// ── Live tape via SSE ──────────────────────────────────────────────────
const TAPE_MAX = 200;
let tapePaused = false;
let tape = [];

function pushTape(ev) {
  if (tapePaused) return;
  tape.unshift(ev);
  if (tape.length > TAPE_MAX) tape.length = TAPE_MAX;
  renderTape();
  // Keep the pulse view's mono tape in sync when it's the visible page.
  // pulseTape is rendered lazily — we only redraw when its container is on.
  const pulseEl = document.getElementById("view-pulse");
  if (pulseEl && !pulseEl.hidden && typeof renderPulseTape === "function") {
    renderPulseTape();
  }
}

function renderTape() {
  const html = tape.map((ev) => {
    let detail = "";
    let extraClass = "";
    switch (ev.phase) {
      case "Started":
        detail = `${ev.mode} · ${fmtBytes(ev.original_size)} · ${ev.file_name || ""}`;
        break;
      case "StageDone":
        if (ev.stage && ev.stage.indexOf("IntegrityCheck:") === 0) {
          // stage_attr_chunk_count is reused as "checked",
          // stage_attr_bucket_count is reused as "mismatches" by IntegrityDiagnostics.
          const checked = ev.stage_attr_chunk_count || 0;
          const mism = ev.stage_attr_bucket_count || 0;
          detail = mism > 0
            ? `❌ ${ev.stage.replace("IntegrityCheck:","")} · ${mism}/${checked} MISMATCH · ${fmtMs(ev.stage_ms)}`
            : `✓ ${ev.stage.replace("IntegrityCheck:","")} · ${checked} ok · ${fmtMs(ev.stage_ms)}`;
          extraClass = mism > 0 ? "integrity-bad" : "integrity-ok";
        } else {
          detail = `${ev.stage} · ${fmtMs(ev.stage_ms)}`;
        }
        break;
      case "BlockDone":
        detail = `block ${ev.block_index + 1}/${ev.block_count} · ${fmtBytes(ev.block_bytes_in)}→${fmtBytes(ev.block_bytes_out)} · ${ev.block_refs_found}refs · ${fmtMs(ev.block_ms)}`;
        break;
      case "Completed":
        detail = `→ ${fmtBytes(ev.compressed_size)} · dc ${fmtBytes(ev.dc_bytes)} · ${ev.refs_found}refs · ${fmtMs(ev.server_ms)} server / ${fmtMs(ev.wall_ms)} wall`;
        break;
      case "Failed":
        detail = `${ev.error_class || "?"} · ${(ev.error_message || "").substring(0, 80)}`;
        if (ev.error_class === "INTEGRITY_DECOMPRESS") extraClass = "integrity-bad";
        break;
    }
    const phaseUpper = (ev.phase || "").replace(/([a-z])([A-Z])/g, "$1_$2").toUpperCase();
    return `<div class="row ${extraClass}">
      <span class="ts">${fmtTs(ev.ts_ns)}</span>
      <span class="phase ${phaseUpper}">${phaseUpper}</span>
      <span class="id" title="${ev.job_id}">${shortJob(ev.job_id)}</span>
      <span class="detail">${detail}</span>
    </div>`;
  }).join("");
  $("tape").innerHTML = html;
}

async function loadInitialTape() {
  try {
    const r = await fetch("/api/events/recent?max=200");
    if (!r.ok) return;
    const events = await r.json();
    tape = events.reverse(); // ring buffer returns oldest first; we display newest first
    renderTape();
  } catch (e) { /* ignore */ }
}

function setLive(state) {
  const dot = $("liveDot");
  const lbl = $("liveLabel");
  dot.classList.remove("live", "dead");
  if (state === "live") { dot.classList.add("live"); lbl.textContent = "live"; }
  else if (state === "dead") { dot.classList.add("dead"); lbl.textContent = "disconnected"; }
  else { lbl.textContent = "connecting…"; }
}

let es = null;
function connectSse() {
  if (es) try { es.close(); } catch (e) {}
  setLive("connecting");
  es = new EventSource("/api/events/stream");
  es.onopen = () => setLive("live");
  es.onerror = () => {
    setLive("dead");
    setTimeout(connectSse, 3000);
  };
  es.onmessage = (m) => {
    try { pushTape(JSON.parse(m.data)); } catch (e) {}
  };
}

// ── Pulse view ─────────────────────────────────────────────────────────
// Rolling throughput sample. We keep the last few snapshots and compute
// bytes-in / bytes-out / completed deltas over the window so the pulse view
// can show meaningful "right now" numbers instead of just totals since boot.
const PULSE_WINDOW_MS = 60_000;
const pulseStartedAt = Date.now();
const pulseHistory = [];

function pulsePushSample(s) {
  const t = Date.now();
  pulseHistory.push({
    t,
    bin: Number(s.kpis.bytes_in || 0),
    bout: Number(s.kpis.bytes_out || 0),
    completed: Number(s.kpis.completed || 0),
  });
  while (pulseHistory.length > 1 && pulseHistory[0].t < t - PULSE_WINDOW_MS) {
    pulseHistory.shift();
  }
}

function pulseThroughput() {
  if (pulseHistory.length < 2) return { binPerSec: 0, boutPerSec: 0, filesPerMin: 0 };
  const first = pulseHistory[0];
  const last = pulseHistory[pulseHistory.length - 1];
  const dt = Math.max(1, (last.t - first.t) / 1000);
  return {
    binPerSec: Math.max(0, (last.bin - first.bin) / dt),
    boutPerSec: Math.max(0, (last.bout - first.bout) / dt),
    filesPerMin: Math.max(0, ((last.completed - first.completed) / dt) * 60),
  };
}

function fmtRate(bytesPerSec) {
  if (!bytesPerSec || !isFinite(bytesPerSec)) return "— /s";
  return fmtBytes(bytesPerSec) + "/s";
}

function fmtUptime(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  const pad = (x) => String(x).padStart(2, "0");
  return `${pad(h)}:${pad(m)}:${pad(ss)}`;
}

function fmtRatioGain(numerator, denominator) {
  // Returns "x.xx×" — how much bigger the numerator is vs denominator.
  // Use case: ccf / dc tells us "we returned X× more bytes than we kept".
  if (!denominator || denominator <= 0) return "—";
  const r = numerator / denominator;
  if (!isFinite(r)) return "—";
  if (r >= 10) return r.toFixed(1) + "×";
  return r.toFixed(2) + "×";
}

function renderPulse(snapshot) {
  pulsePushSample(snapshot);
  const k = snapshot.kpis;

  // hero row
  $("pulseBytesIn").textContent = fmtBytes(k.bytes_in);
  $("pulseBytesOut").textContent = fmtBytes(k.bytes_out);
  $("pulseFiles").textContent = fmtNum(k.completed);
  $("pulseBlocks").textContent = fmtNum(k.blocks);
  $("pulseAvgMs").textContent = fmtMs(k.avg_server_ms);
  $("pulseAvgMs2").textContent = fmtMs(k.avg_server_ms);
  $("pulseActive").textContent = fmtNum(k.active_jobs);
  $("pulseActive2").textContent = fmtNum(k.active_jobs);
  $("pulseCRatio").textContent = fmtRatio(k.compress_ratio);

  // datacenter spotlight
  const dc = Number(k.bytes_dc || 0);
  const ccf = Number(k.bytes_out || 0);
  const bin = Number(k.bytes_in || 0);
  $("pulseDc").textContent = fmtBytes(dc);
  $("pulseCcf").textContent = fmtBytes(ccf);
  $("pulseDcVsCcf").textContent = ccf > 0 ? (dc / ccf * 100).toFixed(1) + "%" : "—";
  $("pulseDcVsIn").textContent = bin > 0 ? (dc / bin * 100).toFixed(1) + "%" : "—";

  // "for every 1 GB ccf, we keep X bytes on dc"
  const oneGb = 1024 * 1024 * 1024;
  if (ccf > 0) {
    const dcPerGbCcf = (dc / ccf) * oneGb;
    $("pulseCcfUnit").textContent = "1.00 GB";
    $("pulseDcUnit").textContent = fmtBytes(dcPerGbCcf);
  } else {
    $("pulseCcfUnit").textContent = "1.00 GB";
    $("pulseDcUnit").textContent = "— B";
  }

  // dedup
  const refs = Number(k.refs_found || 0);
  const chunks = Number(k.chunks || 0);
  const dedupPct = chunks > 0 ? (refs / chunks) * 100 : 0;
  $("pulseDedupPct").textContent = chunks > 0 ? dedupPct.toFixed(1) + "%" : "—";
  $("pulseRefs").textContent = fmtNum(refs);
  $("pulseChunks").textContent = fmtNum(chunks);
  $("pulseDedupFill").style.width = Math.min(100, dedupPct).toFixed(1) + "%";

  // jobs
  $("pulseStarted").textContent = fmtNum(k.started);
  $("pulseCompleted").textContent = fmtNum(k.completed);
  $("pulseCompleted2").textContent = fmtNum(k.completed);
  $("pulseFailed").textContent = fmtNum(k.failed);

  // integrity
  const intChecked = Number(k.integrity_checked || 0);
  const intMism = Number(k.integrity_mismatches || 0);
  const intDec = Number(k.integrity_decompress_failures || 0);
  $("pulseIntChecked").textContent = fmtNum(intChecked);
  $("pulseIntMism").textContent = fmtNum(intMism);
  $("pulseIntDec").textContent = fmtNum(intDec);
  const intState = $("pulseIntegrityState");
  if (intMism > 0 || intDec > 0) {
    intState.textContent = "FAIL";
    intState.className = "pcard-big mono err";
  } else if (intChecked > 0) {
    intState.textContent = "PASS";
    intState.className = "pcard-big mono ok";
  } else {
    intState.textContent = "— —";
    intState.className = "pcard-big mono";
  }

  // throughput
  const t = pulseThroughput();
  $("pulseTput").textContent = `${fmtRate(t.binPerSec)} → ${fmtRate(t.boutPerSec)}`;

  // uptime
  $("pulseUptime").textContent = fmtUptime(Date.now() - pulseStartedAt);
}

function renderPulseTape() {
  // Reuse the existing tape buffer but render with mono-style row layout.
  const slice = tape.slice(0, 30);
  $("pulseTape").innerHTML = slice.map((ev) => {
    let body = "";
    let cls = "";
    switch (ev.phase) {
      case "Started":
        body = `START   ${(ev.file_name || "").substring(0, 48).padEnd(48)} ${fmtBytes(ev.original_size).padStart(10)}`;
        break;
      case "BlockDone":
        body = `BLOCK   ${String(ev.block_index + 1).padStart(3)}/${String(ev.block_count).padEnd(3)}   ${fmtBytes(ev.block_bytes_in).padStart(9)} → ${fmtBytes(ev.block_bytes_out).padStart(9)}   ${String(ev.block_refs_found).padStart(5)}refs   ${fmtMs(ev.block_ms)}`;
        break;
      case "StageDone":
        if (ev.stage && ev.stage.indexOf("IntegrityCheck:") === 0) {
          const mism = ev.stage_attr_bucket_count || 0;
          const checked = ev.stage_attr_chunk_count || 0;
          cls = mism > 0 ? "tx err" : "tx ok";
          body = `INTEG   ${ev.stage.replace("IntegrityCheck:", "").padEnd(20)} ${mism > 0 ? "❌ " + mism + "/" + checked : "✓ " + checked}`;
        } else {
          body = `STAGE   ${(ev.stage || "").padEnd(30)} ${fmtMs(ev.stage_ms)}`;
        }
        break;
      case "Completed":
        cls = "tx ok";
        body = `OK      ${fmtBytes(ev.compressed_size).padStart(10)} dc ${fmtBytes(ev.dc_bytes).padStart(10)}   ${String(ev.refs_found).padStart(5)}refs   ${fmtMs(ev.wall_ms)}`;
        break;
      case "Failed":
        cls = "tx err";
        body = `FAIL    ${(ev.error_class || "?").padEnd(24)} ${(ev.error_message || "").substring(0, 60)}`;
        break;
      default:
        body = ev.phase;
    }
    return `<div class="ptape-row ${cls}"><span class="ptape-t">${fmtTs(ev.ts_ns)}</span>${body.replace(/ /g, "&nbsp;")}</div>`;
  }).join("");
}

// ── View switching ─────────────────────────────────────────────────────
function switchView(name) {
  const buttons = document.querySelectorAll("#viewNav button");
  buttons.forEach((b) => b.classList.toggle("active", b.dataset.view === name));
  document.getElementById("view-operator").hidden = name !== "operator";
  document.getElementById("view-pulse").hidden = name !== "pulse";
  try { localStorage.setItem("crossv9.view", name); } catch (_) {}
  if (name === "pulse") renderPulseTape();
}

document.querySelectorAll("#viewNav button").forEach((b) =>
  b.addEventListener("click", () => switchView(b.dataset.view)));

// Refresh snapshot for the pulse view too. We hook into the existing
// refreshSnapshot via a side-channel: re-fetch on its cadence and render.
async function refreshPulse() {
  try {
    const r = await fetch("/api/snapshot");
    if (!r.ok) return;
    const s = await r.json();
    renderPulse(s);
  } catch (e) { /* ignore */ }
}

// ── Wire up ────────────────────────────────────────────────────────────
$("pauseBtn").addEventListener("click", () => {
  tapePaused = !tapePaused;
  $("pauseBtn").textContent = tapePaused ? "resume" : "pause";
  $("pauseBtn").classList.toggle("paused", tapePaused);
});

// ── Reset stats button ────────────────────────────────────────────────
// Zeroes the monotonic top-bar counters (started/completed/failed/integrity)
// in-place. Without this the only way to clear a stale "failed: 42" left
// over from an earlier OOM run was to restart the controlcenter pod.
const resetBtn = $("resetStatsBtn");
if (resetBtn) {
  resetBtn.addEventListener("click", async () => {
    if (!confirm("Reset all dashboard counters (started/completed/failed/integrity)?\n\nThis only clears the in-memory KPIs. The on-disk event journal and live tape are untouched.")) {
      return;
    }
    const original = resetBtn.textContent;
    resetBtn.disabled = true;
    resetBtn.textContent = "resetting…";
    try {
      const r = await fetch("/api/reset", { method: "POST" });
      if (!r.ok) {
        alert("Reset failed: HTTP " + r.status);
        return;
      }
      // Pull fresh snapshot immediately so the numbers visibly drop to 0.
      await refreshSnapshot();
      await refreshPulse();
    } catch (e) {
      alert("Reset failed: " + e);
    } finally {
      resetBtn.disabled = false;
      resetBtn.textContent = original;
    }
  });
}

(async function init() {
  // Restore last selected view
  try {
    const last = localStorage.getItem("crossv9.view");
    if (last === "pulse" || last === "operator") switchView(last);
  } catch (_) {}

  await Promise.all([loadInitialTape(), refreshSnapshot(), refreshPulse(), refreshFiles(), refreshFleet()]);
  connectSse();
  setInterval(refreshSnapshot, 2000);
  setInterval(refreshPulse, 2000);
  setInterval(refreshFiles, 15000);
  setInterval(refreshFleet, 10000);
})();
