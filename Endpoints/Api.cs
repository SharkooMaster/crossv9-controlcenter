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
                },
                journal = new
                {
                    enqueued = journal.Enqueued,
                    written = journal.Written,
                    dropped = journal.Dropped,
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

        // ── Health ─────────────────────────────────────────────────────
        app.MapGet("/health", () => Results.Ok("ok"));
        app.MapGet("/ready", () => Results.Ok("ready"));
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
