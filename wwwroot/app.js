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
    try {
      const ev = JSON.parse(m.data);
      pushTape(ev);
      topoOnEvent(ev);
    } catch (e) {}
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
  document.getElementById("view-topology").hidden = name !== "topology";
  try { localStorage.setItem("crossv9.view", name); } catch (_) {}
  if (name === "pulse") renderPulseTape();
  if (name === "topology") topoOnShow();
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

// ── Topology view ──────────────────────────────────────────────────────
// Architecture:
//   - The backend's /api/topology returns a node + edge snapshot. We poll it
//     on a slow cadence (3s) — only the structure cares about that.
//   - Live SSE events drive *pulses*: short-lived dots that travel from src→dst
//     along an edge. The pulse list is rate-capped so a benchmark spike can't
//     grind the canvas to 1 fps.
//   - Layout is a manual column-based projection rather than d3-force: every
//     node lives in its component's lane (CLIENT, CROSS, ROUTER, AGENTS), and
//     vertical position is a stable hash of the pod name. This keeps the
//     graph readable when 8+ pods share a lane — better than a drifting force
//     simulation that flips orientation on every refresh.
const TOPO_LANES = ["client", "cross", "router", "agent"];
let topoState = {
  // Last fetched snapshot
  nodes: [],
  edges: [],
  clientId: "_client",
  gatewayId: "_gateway",
  // Layout cache: id → {x, y}
  pos: new Map(),
  // Scratch geometry: edge id ("src→dst") → SVG path d-attribute
  edgePaths: new Map(),
  // Live pulses currently animating: {id, edgeId, kind, startMs, durationMs}
  pulses: [],
  pulseSeq: 0,
  // Currently selected node/edge for the sidebar
  selection: { kind: null, id: null },
  // Recent activity per edge / per node for the sidebar tape (newest first)
  edgeTape: new Map(),     // edgeId → [{ts_ns, phase, body}]
  nodeTape: new Map(),     // nodeId → [{ts_ns, phase, body}]
  shown: false,
  rafActive: false,
  width: 0,
  height: 0,
};
const TOPO_TAPE_MAX = 80;
const TOPO_PULSE_MAX = 200;

const TOPO_LANE_COLORS = {
  client: "#f5b1ff",
  cross: "#4cd0ff",
  router: "#ffb454",
  gateway: "#7a82ff",
  agent: "#5fe89a",
};

function topoLaneOf(node) {
  const id = node.id;
  const comp = node.component || "";
  if (id === topoState.clientId) return "client";
  if (id === topoState.gatewayId) return "router";
  if (comp === "cross") return "cross";
  // Real gateway pods share the "router" lane with the synthetic router node:
  // visually they're the same tier, and we don't render a separate column for
  // them. Without this collapse `grouped["gateway"]` is undefined and the
  // layout pass crashes (TopoLayout: can't access property 'push').
  if (comp === "gateway") return "router";
  if (comp === "agent") return "agent";
  // Fall back to "cross" lane for anything we couldn't classify; keeps the
  // graph from blowing up if a new component shows up before we teach the UI.
  return "cross";
}

function topoStableHash(s) {
  // Cheap deterministic 0..1. Lets us put pods in stable vertical positions
  // across reloads without depending on a force simulation seed.
  let h = 2166136261 >>> 0;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return ((h >>> 0) % 10000) / 10000;
}

async function topoFetch() {
  try {
    const r = await fetch("/api/topology");
    if (!r.ok) return;
    const t = await r.json();
    topoState.nodes = t.nodes || [];
    topoState.edges = t.edges || [];
    topoState.clientId = t.client_node_id;
    topoState.gatewayId = t.gateway_node_id;
    $("topoEventsApplied").textContent = fmtNum(t.events_applied);
    $("topoNodeCount").textContent = fmtNum(topoState.nodes.length);
    $("topoEdgeCount").textContent = fmtNum(topoState.edges.length);
    topoLayout();
    topoRender();
    topoUpdateSelection();
  } catch (e) { /* transient */ }
}

function topoLayout() {
  const svg = $("topoSvg");
  if (!svg) return;
  const rect = svg.getBoundingClientRect();
  const w = rect.width || 1200;
  const h = rect.height || 540;
  topoState.width = w;
  topoState.height = h;
  const padX = Math.min(140, w * 0.12);
  const padY = 60;
  const lanes = TOPO_LANES;
  const laneCount = lanes.length;
  const laneSpacing = (w - 2 * padX) / Math.max(1, laneCount - 1);

  // Group nodes by lane to know how many vertical slots we need per lane.
  const grouped = {};
  for (const lane of lanes) grouped[lane] = [];
  for (const node of topoState.nodes) {
    let lane = topoLaneOf(node);
    // Defensive: if a future component returns an unknown lane name, drop it
    // into "cross" rather than crashing the whole render pass. The console
    // warning makes the misclassification easy to find when adding a new
    // component type.
    if (!grouped[lane]) {
      console.warn(`[topology] unknown lane "${lane}" for node id=${node.id} component=${node.component}; bucketing into cross`);
      lane = "cross";
    }
    grouped[lane].push(node);
  }
  // Stable sort within a lane so successive renders don't shuffle nodes.
  for (const lane of lanes) {
    grouped[lane].sort((a, b) => a.id.localeCompare(b.id));
  }

  topoState.pos.clear();
  lanes.forEach((lane, laneIdx) => {
    const arr = grouped[lane];
    const x = padX + laneIdx * laneSpacing;
    if (arr.length === 1) {
      topoState.pos.set(arr[0].id, { x, y: h / 2 });
      return;
    }
    const usable = h - 2 * padY;
    arr.forEach((node, i) => {
      // Use stable hash to spread but keep order; helps when the lane has many
      // pods and we want them roughly evenly spaced rather than crowding.
      const base = arr.length > 1 ? i / (arr.length - 1) : 0.5;
      const jitter = (topoStableHash(node.id) - 0.5) * 0.06; // ±3% wiggle
      const y = padY + Math.max(0, Math.min(1, base + jitter)) * usable;
      topoState.pos.set(node.id, { x, y });
    });
  });

  // Pre-compute edge paths so animation lookups are O(1).
  topoState.edgePaths.clear();
  for (const e of topoState.edges) {
    const p1 = topoState.pos.get(e.src);
    const p2 = topoState.pos.get(e.dst);
    if (!p1 || !p2) continue;
    const dx = p2.x - p1.x;
    const dy = p2.y - p1.y;
    const c1x = p1.x + dx * 0.45;
    const c1y = p1.y;
    const c2x = p1.x + dx * 0.55;
    const c2y = p2.y;
    const path = `M${p1.x.toFixed(1)},${p1.y.toFixed(1)} C${c1x.toFixed(1)},${c1y.toFixed(1)} ${c2x.toFixed(1)},${c2y.toFixed(1)} ${p2.x.toFixed(1)},${p2.y.toFixed(1)}`;
    topoState.edgePaths.set(`${e.src}→${e.dst}`, { p1, p2, path });
  }
}

function topoEdgeIntensity(e) {
  // Returns a 0..1 weight used to light up "hot" edges. We treat anything that
  // saw activity in the last 8s as fully hot, tapering off to zero by 30s.
  if (!e.last_ts_ns) return 0;
  const ageMs = (Date.now() * 1e6 - Number(e.last_ts_ns)) / 1e6;
  if (ageMs < 8000) return 1;
  if (ageMs > 30000) return 0;
  return 1 - (ageMs - 8000) / 22000;
}

function topoRender() {
  const svg = $("topoSvg");
  if (!svg) return;
  const w = topoState.width;
  const h = topoState.height;
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);

  // Build edge paths first so they sit *under* the nodes.
  let edgesHtml = "";
  for (const e of topoState.edges) {
    const geom = topoState.edgePaths.get(`${e.src}→${e.dst}`);
    if (!geom) continue;
    const intensity = topoEdgeIntensity(e);
    const cls = intensity > 0.6 ? "hot" : (intensity > 0 ? "" : "dim");
    const sel = topoState.selection.kind === "edge" && topoState.selection.id === `${e.src}→${e.dst}`;
    edgesHtml += `<path class="topo-edge ${cls} ${sel ? "selected" : ""}" d="${geom.path}" data-edge="${e.src}→${e.dst}"/>`;
  }

  // Render nodes.
  let nodesHtml = "";
  for (const node of topoState.nodes) {
    const p = topoState.pos.get(node.id);
    if (!p) continue;
    const lane = topoLaneOf(node);
    const sel = topoState.selection.kind === "node" && topoState.selection.id === node.id;
    const dead = node.alive === false;
    // Compute event throughput for the node's headline number — we count
    // outgoing edges' "count" as a rough activity proxy.
    let outCount = 0;
    for (const e of topoState.edges) {
      if (e.src === node.id) outCount += Number(e.count || 0);
    }
    const radius = 26;
    const ringR = 36;
    const labelMain = node.label || "?";
    // The kind label shows what the pod actually IS, not the lane it lives in.
    // Real gateway pods sit in the "router" lane next to the synthetic router
    // node, but should still display "GATEWAY" so operators can tell them
    // apart at a glance.
    const kind = (node.component || lane).toUpperCase();
    const tooltip = `${node.id}\nnode: ${node.node || "—"}\nheap: ${fmtBytes(node.heap_bytes)}\nrss: ${fmtBytes(node.rss_bytes)}\ngc: ${(node.gc_pct || 0).toFixed(2)}%`;

    nodesHtml += `
      <g class="topo-node ${sel ? "selected" : ""} ${dead ? "dead" : ""}" data-node="${node.id}" transform="translate(${p.x.toFixed(1)},${p.y.toFixed(1)})">
        <circle class="topo-node-ring ${dead ? "dead" : "alive"}" r="${ringR}"/>
        <circle class="topo-node-bg ${lane}" r="${radius}">
          <title>${tooltip.replace(/&/g,"&amp;").replace(/</g,"&lt;")}</title>
        </circle>
        <text class="topo-node-count" y="-2">${outCount > 0 ? fmtNum(outCount) : "·"}</text>
        <text class="topo-node-kind" y="12">${kind}</text>
        <text class="topo-node-floatlabel" y="${ringR + 16}">${labelMain}</text>
      </g>
    `;
  }

  svg.innerHTML = `
    <g id="topoEdgesLayer">${edgesHtml}</g>
    <g id="topoNodesLayer">${nodesHtml}</g>
    <g id="topoPulsesLayer"></g>
  `;

  // Wire up clicks. Delegated so a re-render doesn't lose handlers.
  svg.onclick = (ev) => {
    const node = ev.target.closest("[data-node]");
    if (node) {
      topoState.selection = { kind: "node", id: node.dataset.node };
      topoRender();
      topoUpdateSelection();
      return;
    }
    const edge = ev.target.closest("[data-edge]");
    if (edge) {
      topoState.selection = { kind: "edge", id: edge.dataset.edge };
      topoRender();
      topoUpdateSelection();
    }
  };
}

function topoEdgeIdsForEvent(ev) {
  // Map a JobEvent to a list of edges (src→dst) it should pulse along. This
  // mirrors TopologyTracker.Apply on the server, but we have to reconstruct
  // it client-side because pulses need to fire BEFORE the next /api/topology
  // poll catches up with the server-side counters.
  const ids = [];
  if (!ev || !ev.cross_pod) return ids;
  if (ev.phase === "Started") {
    ids.push({ id: `${topoState.clientId}→${ev.cross_pod}`, kind: "kind-start" });
  } else if (ev.phase === "StageDone") {
    const stage = ev.stage || "";
    if (stage === "SearchBuckets" || stage === "StoreChunks" || stage === "BatchGet" || stage === "BatchStore") {
      ids.push({ id: `${ev.cross_pod}→${topoState.gatewayId}`, kind: "kind-stage" });
      // Light up gateway→agent fan-out for any agent we currently see.
      for (const node of topoState.nodes) {
        if (node.component === "agent") {
          ids.push({ id: `${topoState.gatewayId}→${node.id}`, kind: "kind-stage" });
        }
      }
    }
  } else if (ev.phase === "Completed") {
    ids.push({ id: `${ev.cross_pod}→${topoState.clientId}`, kind: "kind-done" });
  } else if (ev.phase === "Failed") {
    ids.push({ id: `${ev.cross_pod}→${topoState.clientId}`, kind: "kind-error" });
  }
  return ids;
}

function topoSpawnPulse(edgeId, kindClass) {
  const geom = topoState.edgePaths.get(edgeId);
  if (!geom) return;
  if (topoState.pulses.length > TOPO_PULSE_MAX) {
    // Drop oldest. Trade truthful representation of every event for keeping
    // the canvas at 60 fps during a benchmark.
    topoState.pulses.shift();
  }
  topoState.pulses.push({
    id: ++topoState.pulseSeq,
    edgeId,
    kindClass,
    startMs: performance.now(),
    durationMs: 700 + Math.random() * 200,
  });
  $("topoLivePulses").textContent = String(topoState.pulses.length);
  if (!topoState.rafActive) {
    topoState.rafActive = true;
    requestAnimationFrame(topoTickPulses);
  }
}

function topoTapeEntryFor(ev) {
  let body = "";
  let cls = "";
  if (ev.phase === "Started") {
    body = `${ev.mode || ""} ${fmtBytes(ev.original_size)} ${ev.file_name || ""}`.trim();
  } else if (ev.phase === "StageDone") {
    body = `${ev.stage || ""} ${fmtMs(ev.stage_ms)}`;
  } else if (ev.phase === "Completed") {
    body = `→ ${fmtBytes(ev.compressed_size)} · ${fmtNum(ev.refs_found)}refs · ${fmtMs(ev.wall_ms)}`;
    cls = "ok";
  } else if (ev.phase === "Failed") {
    body = `${ev.error_class || "?"} ${(ev.error_message || "").substring(0, 60)}`;
    cls = "err";
  } else {
    body = ev.phase;
  }
  return { ts_ns: ev.ts_ns, phase: ev.phase, body, cls };
}

function topoRecordTape(map, key, ev) {
  let arr = map.get(key);
  if (!arr) { arr = []; map.set(key, arr); }
  arr.unshift(topoTapeEntryFor(ev));
  if (arr.length > TOPO_TAPE_MAX) arr.length = TOPO_TAPE_MAX;
}

function topoOnEvent(ev) {
  if (!ev) return;
  // Always record into the per-edge / per-node tape so the sidebar has
  // history even before the user opens the topology view.
  const edges = topoEdgeIdsForEvent(ev);
  for (const e of edges) topoRecordTape(topoState.edgeTape, e.id, ev);
  if (ev.cross_pod) topoRecordTape(topoState.nodeTape, ev.cross_pod, ev);

  if (!topoState.shown) return; // don't animate when the view isn't visible
  for (const e of edges) topoSpawnPulse(e.id, e.kindClass);

  // If selection is showing this edge/node, refresh the sidebar tape.
  if (topoState.selection.kind === "edge" && edges.some((e) => e.id === topoState.selection.id)) {
    topoUpdateSelection();
  } else if (topoState.selection.kind === "node" && ev.cross_pod === topoState.selection.id) {
    topoUpdateSelection();
  }
}

function topoTickPulses(nowMs) {
  const layer = document.getElementById("topoPulsesLayer");
  if (!layer) {
    topoState.rafActive = false;
    return;
  }
  let html = "";
  let alive = 0;
  for (const p of topoState.pulses) {
    const t = (nowMs - p.startMs) / p.durationMs;
    if (t >= 1) continue;
    const geom = topoState.edgePaths.get(p.edgeId);
    if (!geom) continue;
    // Linear interpolate along the bezier control polyline — close enough at
    // a 60 fps tick to look like the pulse follows the curve.
    const { p1, p2 } = geom;
    const dx = p2.x - p1.x;
    const dy = p2.y - p1.y;
    // Smooth ease-out so pulses arrive softly.
    const eased = 1 - Math.pow(1 - t, 2);
    const x = p1.x + dx * eased;
    const y = p1.y + dy * eased;
    const r = 4.5;
    html += `<circle class="topo-pulse ${p.kindClass}" cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${r}"/>`;
    alive++;
  }
  layer.innerHTML = html;
  // Filter out finished pulses to keep memory steady.
  if (alive !== topoState.pulses.length) {
    topoState.pulses = topoState.pulses.filter((p) => (nowMs - p.startMs) < p.durationMs);
    $("topoLivePulses").textContent = String(topoState.pulses.length);
  }
  if (topoState.pulses.length > 0) {
    requestAnimationFrame(topoTickPulses);
  } else {
    topoState.rafActive = false;
  }
}

function topoUpdateSelection() {
  const sel = topoState.selection;
  if (!sel.kind) {
    $("topoSelKind").textContent = "no selection";
    $("topoSelTitle").textContent = "click a node or edge";
    $("topoSelMeta").textContent = "";
    $("topoSelTape").innerHTML = `<div class="empty">nothing selected</div>`;
    return;
  }
  if (sel.kind === "node") {
    const node = topoState.nodes.find((n) => n.id === sel.id);
    if (!node) return;
    $("topoSelKind").textContent = `node · ${(node.component || "?")}`;
    $("topoSelTitle").textContent = node.label || node.id;
    let outCount = 0, inCount = 0, outBytes = 0, inBytes = 0;
    for (const e of topoState.edges) {
      if (e.src === node.id) { outCount += Number(e.count || 0); outBytes += Number(e.bytes || 0); }
      if (e.dst === node.id) { inCount += Number(e.count || 0); inBytes += Number(e.bytes || 0); }
    }
    $("topoSelMeta").innerHTML = `
      <div>id: ${node.id}</div>
      <div>k8s node: ${node.node || "—"}</div>
      <div>heap: ${fmtBytes(node.heap_bytes)} · rss: ${fmtBytes(node.rss_bytes)} · gc ${(node.gc_pct || 0).toFixed(2)}%</div>
      <div>in: ${fmtNum(inCount)} ev · ${fmtBytes(inBytes)}</div>
      <div>out: ${fmtNum(outCount)} ev · ${fmtBytes(outBytes)}</div>
      <div>state: ${node.alive ? "alive" : "stale/dead"}</div>
    `;
    const tape = topoState.nodeTape.get(node.id) || [];
    $("topoSelTape").innerHTML = topoRenderTape(tape);
    return;
  }
  if (sel.kind === "edge") {
    const [src, dst] = sel.id.split("→");
    const edge = topoState.edges.find((e) => e.src === src && e.dst === dst);
    if (!edge) {
      $("topoSelKind").textContent = "edge";
      $("topoSelTitle").textContent = sel.id;
      $("topoSelMeta").textContent = "no aggregate stats yet";
      $("topoSelTape").innerHTML = `<div class="empty">no events recorded on this edge yet</div>`;
      return;
    }
    $("topoSelKind").textContent = "edge";
    $("topoSelTitle").textContent = `${src} → ${dst}`;
    const ageMs = edge.last_ts_ns ? ((Date.now() * 1e6 - Number(edge.last_ts_ns)) / 1e6) : null;
    $("topoSelMeta").innerHTML = `
      <div>events: ${fmtNum(edge.count)}</div>
      <div>bytes: ${fmtBytes(edge.bytes)}</div>
      <div>last stage: ${edge.last_stage || "—"}</div>
      <div>last seen: ${ageMs == null ? "never" : (ageMs < 1000 ? "just now" : (ageMs / 1000).toFixed(0) + "s ago")}</div>
    `;
    const tape = topoState.edgeTape.get(sel.id) || [];
    $("topoSelTape").innerHTML = topoRenderTape(tape);
  }
}

function topoRenderTape(arr) {
  if (!arr || arr.length === 0) {
    return `<div class="empty">no events recorded yet</div>`;
  }
  return arr.map((r) => {
    const phase = (r.phase || "").replace(/([a-z])([A-Z])/g, "$1_$2").toUpperCase();
    return `<div class="row ${r.cls || ""}"><span class="ts">${fmtTs(r.ts_ns)}</span><span class="ph">${phase}</span><span class="body">${r.body}</span></div>`;
  }).join("");
}

function topoOnShow() {
  topoState.shown = true;
  // First time the user opens the view, do a full layout pass so the SVG
  // gets sized against the now-visible container.
  topoLayout();
  topoRender();
  topoUpdateSelection();
}

window.addEventListener("resize", () => {
  if (!topoState.shown) return;
  topoLayout();
  topoRender();
});

// ── Bootstrap ──────────────────────────────────────────────────────────
(async function init() {
  // Restore last selected view
  try {
    const last = localStorage.getItem("crossv9.view");
    if (last === "pulse" || last === "operator" || last === "topology") switchView(last);
  } catch (_) {}

  await Promise.all([
    loadInitialTape(),
    refreshSnapshot(),
    refreshPulse(),
    refreshFiles(),
    refreshFleet(),
    topoFetch(),
  ]);
  connectSse();
  setInterval(refreshSnapshot, 2000);
  setInterval(refreshPulse, 2000);
  setInterval(refreshFiles, 15000);
  setInterval(refreshFleet, 10000);
  setInterval(topoFetch, 3000);
})();

