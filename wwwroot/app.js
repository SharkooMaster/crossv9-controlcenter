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
}

function renderTape() {
  const html = tape.map((ev) => {
    let detail = "";
    switch (ev.phase) {
      case "Started":
        detail = `${ev.mode} · ${fmtBytes(ev.original_size)} · ${ev.file_name || ""}`;
        break;
      case "StageDone":
        detail = `${ev.stage} · ${fmtMs(ev.stage_ms)}`;
        break;
      case "BlockDone":
        detail = `block ${ev.block_index + 1}/${ev.block_count} · ${fmtBytes(ev.block_bytes_in)}→${fmtBytes(ev.block_bytes_out)} · ${ev.block_refs_found}refs · ${fmtMs(ev.block_ms)}`;
        break;
      case "Completed":
        detail = `→ ${fmtBytes(ev.compressed_size)} · dc ${fmtBytes(ev.dc_bytes)} · ${ev.refs_found}refs · ${fmtMs(ev.server_ms)} server / ${fmtMs(ev.wall_ms)} wall`;
        break;
      case "Failed":
        detail = `${ev.error_class || "?"} · ${(ev.error_message || "").substring(0, 80)}`;
        break;
    }
    const phaseUpper = (ev.phase || "").replace(/([a-z])([A-Z])/g, "$1_$2").toUpperCase();
    return `<div class="row">
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

// ── Wire up ────────────────────────────────────────────────────────────
$("pauseBtn").addEventListener("click", () => {
  tapePaused = !tapePaused;
  $("pauseBtn").textContent = tapePaused ? "resume" : "pause";
  $("pauseBtn").classList.toggle("paused", tapePaused);
});

(async function init() {
  await Promise.all([loadInitialTape(), refreshSnapshot(), refreshFiles(), refreshFleet()]);
  connectSse();
  setInterval(refreshSnapshot, 2000);
  setInterval(refreshFiles, 15000);
  setInterval(refreshFleet, 10000);
})();
