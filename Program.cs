using Controlcenter.Endpoints;
using Controlcenter.Services;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// Single pod, single instance of every sink. Memory budget for the whole pod
// targets ≤ 100 MB managed at idle:
//   - Journal: ~3 MB (channel + buffered gzip stream)
//   - Ring buffer: ~512 KB
//   - Aggregator: O(active jobs), bounded by stale sweep
//   - Subscribers: ~128 KB per open browser tab
//   - .NET runtime: ~50–60 MB
builder.Services.AddSingleton<JournalWriter>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<JournalWriter>());
builder.Services.AddSingleton<JobEventRingBuffer>(_ => new JobEventRingBuffer(2000));
builder.Services.AddSingleton<LiveBroadcaster>();
builder.Services.AddSingleton<JobAggregator>();
// Topology graph: keeps a rolling map of node↔node edges built from job-event
// flow, surfaced via /api/topology and animated by the SSE pulses on the
// dashboard's topology view.
builder.Services.AddSingleton<TopologyTracker>();

// Saturation history: samples the cumulative ingested / stored / returned totals
// into a bounded time series (default every 5 s, ring of 4096) so the dashboard's
// saturation view can chart whether physical storage growth flattens over time.
builder.Services.AddSingleton<SaturationSampler>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SaturationSampler>());

// Pull-based fleet health: the scraper resolves headless service DNS for
// agent / cross / gateway and hits each pod's /stats/runtime every 30 s.
// Workload pods don't push anything; if this scraper is offline they're
// unaffected.
builder.Services.AddSingleton<RuntimeStatsStore>();
builder.Services.AddHostedService<RuntimeStatsScraper>();
// Journals the latest fleet sample once per minute (cadence configurable via
// FLEET_JOURNAL_INTERVAL_SEC) into the same daily gzip NDJSON file as job
// events. The downloadable journal therefore contains both:
//   - {"kind":"job_event", ...}    — pipeline activity
//   - {"kind":"perf_sample", ...}  — heap / GC / threadpool / RocksDB / FD / cache / stage rollups
// Filter post-hoc with `jq -c 'select(.kind == "perf_sample")'`.
builder.Services.AddHostedService<FleetSampleJournaler>();

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 4 * 1024 * 1024;
    options.MaxSendMessageSize = 1 * 1024 * 1024;
});

// Kestrel: HTTP/2 (gRPC) on 5000, HTTP/1 (UI + REST + SSE) on 5001.
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
});

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();
app.UseRouting();

app.MapGrpcService<JobEventReceiverService>();
app.MapApi();
app.MapGet("/", () => Results.Redirect("/index.html"));

app.Run();
