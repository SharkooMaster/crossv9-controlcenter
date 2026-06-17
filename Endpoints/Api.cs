using System.Text;
using System.Text.Json;
using Controlcenter.Services;
using Crossv9.Jobevents;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Controlcenter.Endpoints;

public static class Api
{
    private static readonly JsonSerializerOptions _json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false,
    };

    public static void MapApi(this WebApplication app)
    {
        // ── KPI snapshot ────────────────────────────────────────────────
        app.MapGet("/api/snapshot", (JobAggregator agg, JournalWriter journal, LiveBroadcaster bus) =>
        {
            var snap = agg.CurrentSnapshot();
            return Results.Json(new
            {
                kpis = new
                {
                    started = snap.TotalStarted,
                    completed = snap.TotalCompleted,
                    failed = snap.TotalFailed,
                    blocks = snap.TotalBlocks,
                    bytes_in = snap.TotalOriginalBytes,
                    bytes_out = snap.TotalCompressedBytes,
                    bytes_dc = snap.TotalDcBytes,
                    refs_found = snap.TotalRefsFound,
                    chunks = snap.TotalChunks,
                    dedup_ratio = snap.TotalChunks > 0 ? (double)snap.TotalRefsFound / snap.TotalChunks : 0.0,
                    compress_ratio = snap.TotalOriginalBytes > 0 ? (double)snap.TotalCompressedBytes / snap.TotalOriginalBytes : 0.0,
                    dc_ratio = snap.TotalOriginalBytes > 0 ? (double)snap.TotalDcBytes / snap.TotalOriginalBytes : 0.0,
                    avg_server_ms = snap.TotalCompleted > 0 ? (double)snap.TotalServerMs / snap.TotalCompleted : 0.0,
                    active_jobs = snap.ActiveJobs.Length,
                    integrity_checked = snap.IntegrityChecked,
                    integrity_mismatches = snap.IntegrityMismatches,
                    integrity_decompress_failures = snap.IntegrityDecompressFailures,
                    integrity_last_ts_ns = snap.IntegrityLastTsNs,
                    integrity_last_detail = snap.IntegrityLastDetail,
                    refroundtrip_checked = snap.RefRoundTripChecked,
                    refroundtrip_mismatches = snap.RefRoundTripMismatches,
                    refroundtrip_fetch_failures = snap.RefRoundTripFetchFailures,
                    refroundtrip_last_ts_ns = snap.RefRoundTripLastTsNs,
                    refroundtrip_last_detail = snap.RefRoundTripLastDetail,
                },
                journal = new
                {
                    enqueued = journal.Enqueued,
                    written = journal.Written,
                    dropped = journal.Dropped,
                    perf_enqueued = journal.PerfEnqueued,
                    perf_dropped = journal.PerfDropped,
                    bytes_written = journal.BytesWritten,
                    directory = journal.JournalDirectory,
                },
                bus = new { sse_subscribers = bus.SubscriberCount },
                active_jobs = snap.ActiveJobs.Select(j => new
                {
                    job_id = j.JobId,
                    file_name = j.FileName,
                    original_size = j.OriginalSize,
                    mode = j.Mode.ToString(),
                    started_ts_ns = j.StartedTsNs,
                    cross_pod = j.CrossPod,
                    blocks_done = j.BlocksDone,
                    block_count = j.BlockCount,
                    bytes_in = j.BytesIn,
                    bytes_out = j.BytesOut,
                    refs_found = j.RefsFound,
                    chunks = j.Chunks,
                    dc_bytes = j.DcBytes,
                    last_stage = j.LastStage,
                }),
            }, _json);
        });

        // ── Saturation time series ──────────────────────────────────────
        // Three cumulative lines — ingested / stored-on-DC / returned-to-customer
        // — plus a summary whose headline number is the *marginal* store rate
        // (Δstored ÷ Δingested over the most recent slice of ingest). That's the
        // signal that tells you whether the global dictionary is saturating:
        // it should fall well below the overall store ratio (and toward 0) as
        // similar data accumulates.
        app.MapGet("/api/saturation", (SaturationSampler sat, int? max) =>
        {
            var samples = sat.Snapshot(Math.Clamp(max ?? 1500, 2, 6000));
            var last = samples.Count > 0 ? samples[^1] : default;
            long totIn = last.BytesIn, totDc = last.BytesDc, totOut = last.BytesOut;

            double marginal = ComputeMarginalStoreRate(samples, out long windowIn, out long windowDc);

            return Results.Json(new
            {
                samples = samples.Select(s => new
                {
                    ts_ms = s.TsMs,
                    bytes_in = s.BytesIn,
                    bytes_dc = s.BytesDc,
                    bytes_out = s.BytesOut,
                    chunks = s.Chunks,
                    refs = s.Refs,
                }),
                summary = new
                {
                    bytes_in = totIn,
                    bytes_dc = totDc,
                    bytes_out = totOut,
                    store_ratio = totIn > 0 ? (double)totDc / totIn : 0.0,    // dc ÷ ingested (overall)
                    return_ratio = totIn > 0 ? (double)totOut / totIn : 0.0,  // ccf ÷ ingested (overall)
                    leverage = totDc > 0 ? (double)totIn / totDc : 0.0,       // logical ÷ physical
                    marginal_store_rate = marginal,                          // recent Δdc ÷ Δin
                    marginal_window_in = windowIn,
                    marginal_window_dc = windowDc,
                    sample_count = samples.Count,
                },
            }, _json);
        });

        // ── Recent events (live tape, oldest first) ────────────────────
        app.MapGet("/api/events/recent", (JobEventRingBuffer ring, int? max) =>
        {
            var limit = Math.Clamp(max ?? 200, 1, ring.Capacity);
            var events = ring.Snapshot(limit).Select(SerializeEvent);
            return Results.Json(events, _json);
        });

        // ── SSE stream of live events ──────────────────────────────────
        app.MapGet("/api/events/stream", async (HttpContext ctx, LiveBroadcaster bus, CancellationToken ct) =>
        {
            ctx.Response.Headers["Content-Type"] = "text/event-stream";
            ctx.Response.Headers["Cache-Control"] = "no-cache";
            ctx.Response.Headers["X-Accel-Buffering"] = "no"; // bypass nginx/etc buffering

            using var sub = bus.Subscribe();
            try
            {
                while (await sub.Reader.WaitToReadAsync(ct))
                {
                    while (sub.Reader.TryRead(out var ev))
                    {
                        var json = JsonSerializer.Serialize(SerializeEvent(ev), _json);
                        await ctx.Response.WriteAsync($"data: {json}\n\n", ct);
                    }
                    await ctx.Response.Body.FlushAsync(ct);
                }
            }
            catch (OperationCanceledException) { /* tab closed */ }
        });

        // ── Journal file listing + download ────────────────────────────
        app.MapGet("/api/files", (JournalWriter journal) =>
        {
            return Results.Json(journal.ListFiles().Select(f => new
            {
                name = f.Name,
                size_bytes = f.SizeBytes,
                last_write_utc = f.LastWriteUtc,
            }), _json);
        });

        app.MapGet("/api/files/{name}", (JournalWriter journal, string name) =>
        {
            try
            {
                var path = journal.ResolveFilePath(name);
                if (!File.Exists(path)) return Results.NotFound();
                return Results.File(path, contentType: "application/gzip", fileDownloadName: name);
            }
            catch (InvalidOperationException) { return Results.BadRequest(); }
        });

        // ── Fleet runtime stats (per-pod heap / fragmentation / RSS / threadpool / GC% / RocksDB) ─
        app.MapGet("/api/fleet", (RuntimeStatsStore store) =>
        {
            var fleet = store.CurrentFleet();
            var now = DateTime.UtcNow;
            return Results.Json(fleet.Select(s => new
            {
                component               = s.Component,
                pod                     = s.Pod,
                node                    = s.Node,
                ip                      = s.ScrapedFromIp,
                ts_unix_ms              = s.TsUnixMs,
                age_sec                 = (now - s.UpdatedUtc).TotalSeconds,
                error                   = s.Error,
                process_uptime_sec      = s.ProcessUptimeSec,

                heap_size_bytes         = s.HeapSizeBytes,
                heap_committed_bytes    = s.HeapCommittedBytes,
                heap_fragmented_bytes   = s.HeapFragmentedBytes,
                heap_fragment_ratio     = s.HeapSizeBytes > 0
                                            ? (double)s.HeapFragmentedBytes / s.HeapSizeBytes : 0.0,
                gen0_size               = s.Gen0Size,
                gen1_size               = s.Gen1Size,
                gen2_size               = s.Gen2Size,
                loh_size                = s.LohSize,
                loh_fragmented_bytes    = s.LohFragmentedBytes,
                poh_size                = s.PohSize,
                gen0_collections        = s.Gen0Collections,
                gen1_collections        = s.Gen1Collections,
                gen2_collections        = s.Gen2Collections,
                gc_pause_total_sec      = s.GcPauseTotalSec,
                time_in_gc_pct          = s.TimeInGcPct,

                rss_bytes               = s.RssBytes,
                private_bytes           = s.PrivateBytes,
                memory_load_bytes       = s.MemoryLoadBytes,
                memory_load_threshold   = s.MemoryLoadThreshold,
                native_overhead_bytes   = s.NativeOverheadBytes,

                threadpool_workers_busy       = s.ThreadpoolWorkersBusy,
                threadpool_workers_max        = s.ThreadpoolWorkersMax,
                threadpool_workers_min        = s.ThreadpoolWorkersMin,
                threadpool_completion_busy    = s.ThreadpoolCompletionBusy,
                threadpool_completion_max     = s.ThreadpoolCompletionMax,
                threadpool_completion_min     = s.ThreadpoolCompletionMin,
                threadpool_queue_length       = s.ThreadpoolQueueLength,

                open_fd_count                = s.OpenFdCount,
                grpc_channel_cache_count     = s.GrpcChannelCacheCount,
                grpc_client_cache_count      = s.GrpcClientCacheCount,

                stage_stats                  = s.StageStats,

                rocks_unique_chunks          = s.RocksUniqueChunks,
                rocks_total_chunk_bytes      = s.RocksTotalChunkBytes,
                rocks_total_buckets          = s.RocksTotalBuckets,
                rocks_total_vectors          = s.RocksTotalVectors,
                rocksdb_chunk                = s.RocksdbChunk,
                rocksdb_bucket               = s.RocksdbBucket,

                // Replication health — surfaced in the fleet table's
                // "Replication" column. ring_size shows how many agents
                // are visible to this cross/gateway pod; topology
                // version + replication factor + write quorum let the
                // operator spot config drift between pods.
                ring_size                    = s.RingSize,
                topology_version             = s.TopologyVersion,
                replication_factor           = s.ReplicationFactor,
                write_quorum                 = s.WriteQuorum,
                vnodes_per_agent             = s.VnodesPerAgent,
                rebalance_coordinator_enabled = s.RebalanceCoordinatorEnabled,
                anti_entropy_enabled         = s.AntiEntropyEnabled,
                anti_entropy_auto_repair     = s.AntiEntropyAutoRepair,
            }), _json);
        });

        // ── Manual fleet reset — drops every cached pod entry. Next scrape
        //    cycle will repopulate only the pods that actually answer. Useful
        //    after a heavy rolling restart when the operator doesn't want to
        //    wait for the FLEET_EVICT_GRACE_SEC window to expire. ────────────
        app.MapPost("/api/fleet/reset", (RuntimeStatsStore store) =>
        {
            int dropped = store.EvictStale(TimeSpan.Zero);
            return Results.Json(new { dropped }, _json);
        });

        // ── Reset KPI counters (top-bar started/completed/failed, integrity,
        //    ref-roundtrip, active in-flight set). The counters are in-memory
        //    and monotonic, so without this they only zero when the pod
        //    restarts — that's how 42 stale failures from an earlier OOM run
        //    keep showing on the dashboard after `reset.sh` wipes the agents
        //    but leaves the controlcenter pod alone. ──────────────────────────
        app.MapPost("/api/reset", (JobAggregator agg, TopologyTracker topo, SaturationSampler sat) =>
        {
            agg.ResetAll();
            topo.Reset();
            sat.Reset();
            return Results.Json(new { reset = true }, _json);
        });

        // ── Topology snapshot (nodes + aggregate edges) ─────────────────
        // The dashboard's topology view polls this every couple of seconds for
        // structure and overlays live SSE events as travelling pulses. Edges
        // are derived from the cross-side stage stream, so they're synthetic
        // (we don't yet trace gateway↔agent RPCs at the wire), but they're an
        // honest "where bytes flowed" map of the request graph.
        app.MapGet("/api/topology", (TopologyTracker topo) =>
        {
            var snap = topo.Snapshot();
            return Results.Json(new
            {
                now_unix_ms = snap.NowUnixMs,
                events_applied = snap.EventsApplied,
                client_node_id = TopologyTracker.ClientNodeId,
                gateway_node_id = TopologyTracker.GatewayAggregateId,
                nodes = snap.Nodes.Select(n => new
                {
                    id = n.Id,
                    component = n.Component,
                    label = n.ShortLabel,
                    node = n.Node,
                    alive = n.Alive,
                    heap_bytes = n.HeapBytes,
                    rss_bytes = n.RssBytes,
                    gc_pct = n.GcPct,
                }),
                edges = snap.Edges.Select(e => new
                {
                    src = e.Src,
                    dst = e.Dst,
                    count = e.Count,
                    bytes = e.Bytes,
                    last_ts_ns = e.LastTsNs,
                    last_stage = e.LastStage,
                }),
            }, _json);
        });

        // ── Per-pod history (rolling 1h at default cadence) ────────────
        app.MapGet("/api/fleet/{component}/{podOrIp}/history",
            (RuntimeStatsStore store, string component, string podOrIp, int? max) =>
        {
            var samples = store.History(component, podOrIp, Math.Clamp(max ?? 120, 1, 1000));
            return Results.Json(samples.Select(s => new
            {
                ts_unix_ms              = s.TsUnixMs,
                heap_size_bytes         = s.HeapSizeBytes,
                heap_fragmented_bytes   = s.HeapFragmentedBytes,
                heap_fragment_ratio     = s.HeapSizeBytes > 0
                                            ? (double)s.HeapFragmentedBytes / s.HeapSizeBytes : 0.0,
                rss_bytes               = s.RssBytes,
                gen2_collections        = s.Gen2Collections,
                native_overhead_bytes   = s.NativeOverheadBytes,
                time_in_gc_pct          = s.TimeInGcPct,
                threadpool_workers_busy = s.ThreadpoolWorkersBusy,
                threadpool_queue_length = s.ThreadpoolQueueLength,
                open_fd_count           = s.OpenFdCount,
                grpc_channel_cache_count = s.GrpcChannelCacheCount,
                rocks_total_chunk_bytes = s.RocksTotalChunkBytes,
                rocks_total_vectors     = s.RocksTotalVectors,
                // Aggregate RocksDB pressure indicators into single scalars so a
                // history chart can plot one line per pressure dimension.
                rocksdb_compaction_pending = (s.RocksdbChunk?.CompactionPending == true)
                                          || (s.RocksdbBucket?.CompactionPending == true),
                rocksdb_write_stopped     = (s.RocksdbChunk?.WriteStopped == true)
                                          || (s.RocksdbBucket?.WriteStopped == true),
                rocksdb_pending_compaction_bytes = (s.RocksdbChunk?.PendingCompactionBytes ?? 0)
                                                 + (s.RocksdbBucket?.PendingCompactionBytes ?? 0),
                rocksdb_l0_files          = (s.RocksdbChunk?.NumFilesPerLevel?.Length > 0
                                                ? s.RocksdbChunk.NumFilesPerLevel[0] : 0)
                                          + (s.RocksdbBucket?.NumFilesPerLevel?.Length > 0
                                                ? s.RocksdbBucket.NumFilesPerLevel[0] : 0),
            }), _json);
        });

        // ── Health ─────────────────────────────────────────────────────
        app.MapGet("/health", () => Results.Ok("ok"));
        app.MapGet("/ready", () => Results.Ok("ready"));
    }

    /// <summary>
    /// Marginal store rate = Δstored ÷ Δingested across the trailing slice of the
    /// series that covers roughly the last 20% of ingested bytes (at least the last
    /// 8 samples). Measuring against an ingest delta — not wall-clock — keeps it
    /// robust to idle gaps. Returns 0 when there isn't enough movement yet.
    /// </summary>
    private static double ComputeMarginalStoreRate(
        List<SaturationSampler.Sample> s, out long windowIn, out long windowDc)
    {
        windowIn = 0; windowDc = 0;
        if (s.Count < 2) return 0.0;

        long totIn = s[^1].BytesIn;
        if (totIn <= 0) return 0.0;

        long targetSpan = (long)(totIn * 0.20);
        int minBack = Math.Min(8, s.Count - 1);
        int idx = s.Count - 1;
        int back = 0;
        for (int i = s.Count - 2; i >= 0; i--)
        {
            idx = i;
            back++;
            if ((totIn - s[i].BytesIn) >= targetSpan && back >= minBack) break;
        }

        windowIn = s[^1].BytesIn - s[idx].BytesIn;
        windowDc = s[^1].BytesDc - s[idx].BytesDc;
        if (windowDc < 0) windowDc = 0;
        return windowIn > 0 ? (double)windowDc / windowIn : 0.0;
    }

    private static object SerializeEvent(JobEvent ev) => new
    {
        job_id = ev.JobId,
        parent_job_id = string.IsNullOrEmpty(ev.ParentJobId) ? null : ev.ParentJobId,
        cross_pod = ev.CrossPod,
        cross_node = ev.CrossNode,
        ts_ns = ev.TsUnixNs,
        phase = ev.Phase.ToString(),
        mode = ev.Mode.ToString(),
        file_name = string.IsNullOrEmpty(ev.FileName) ? null : ev.FileName,
        original_size = ev.OriginalSize,
        stage = string.IsNullOrEmpty(ev.Stage) ? null : ev.Stage,
        stage_ms = ev.StageMs,
        stage_attr_chunk_count = ev.StageAttrChunkCount,
        stage_attr_bucket_count = ev.StageAttrBucketCount,
        stage_attr_bytes = ev.StageAttrBytes,
        block_index = ev.BlockIndex,
        block_count = ev.BlockCount,
        block_bytes_in = ev.BlockBytesIn,
        block_bytes_out = ev.BlockBytesOut,
        block_refs_found = ev.BlockRefsFound,
        block_chunks = ev.BlockChunks,
        block_dc_bytes = ev.BlockDcBytes,
        block_ms = ev.BlockMs,
        compressed_size = ev.FinalCompressedSize,
        refs_found = ev.FinalRefsFound,
        chunks = ev.FinalChunks,
        dc_bytes = ev.FinalDcBytes,
        server_ms = ev.FinalServerMs,
        wall_ms = ev.FinalWallMs,
        avg_error_rate = ev.FinalAvgErrorRate,
        error_payload_bytes = ev.FinalErrorPayloadBytes,
        file_id = string.IsNullOrEmpty(ev.FinalFileId) ? null : ev.FinalFileId,
        error_class = string.IsNullOrEmpty(ev.ErrorClass) ? null : ev.ErrorClass,
        error_message = string.IsNullOrEmpty(ev.ErrorMessage) ? null : ev.ErrorMessage,
        error_stage = string.IsNullOrEmpty(ev.ErrorStage) ? null : ev.ErrorStage,
    };
}
