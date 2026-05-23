using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace Controlcenter.Services;

/// <summary>
/// Last-known runtime sample per (component, pod) pair, plus a small rolling
/// history per pod. Memory-bounded: at most <see cref="HistoryDepth"/> samples
/// per pod (~5 KB/pod across all retained samples).
/// </summary>
public sealed class RuntimeStatsStore
{
    public const int HistoryDepth = 120; // 120 × 30 s = 1 hour rolling

    private readonly ConcurrentDictionary<string, PodEntry> _byKey = new();

    public string UpdateOk(string component, string ip, RuntimeSample sample)
    {
        var key = BuildKey(component, sample.Pod ?? ip);
        var entry = _byKey.GetOrAdd(key, _ => new PodEntry());
        entry.Apply(sample with
        {
            Component = component,
            ScrapedFromIp = ip,
            UpdatedUtc = DateTime.UtcNow,
            Error = null,
        });

        // ── De-duplication on first-successful-scrape ─────────────────────
        // The cross/gateway headless services run with
        // publishNotReadyAddresses: true so peers don't hit NXDOMAIN while a
        // pod is still booting. That means pod IPs land in DNS before
        // Kestrel binds on :5001, and the scraper's first cycle for a new
        // pod records "ECONNREFUSED" against an IP-keyed row (we hadn't
        // learned the pod name yet — that comes from /stats/runtime's
        // response body). Once Kestrel comes up the next cycle succeeds and
        // creates a pod-name-keyed row above. Without this cleanup the
        // IP-keyed row would be orphaned forever — Retain()'s grace window
        // never trips because the IP is still in DNS — and the dashboard
        // would show the same pod twice (one healthy, one stuck on
        // "HTTP CONNECTION REFUSED").
        //
        // We only remove the IP-keyed shadow when the success row uses a
        // *different* key (i.e. sample.Pod was non-null and != ip). When
        // /stats/runtime didn't include a pod name, key == ipKey and there
        // is no shadow to remove.
        if (sample.Pod is not null)
        {
            var ipKey = BuildKey(component, ip);
            if (!string.Equals(ipKey, key, StringComparison.Ordinal))
            {
                _byKey.TryRemove(ipKey, out _);
            }
        }

        return key;
    }

    public string MarkError(string component, string ip, string? pod, string error)
    {
        var key = BuildKey(component, pod ?? ip);
        var entry = _byKey.GetOrAdd(key, _ => new PodEntry());
        entry.MarkError(component, ip, pod, error);
        return key;
    }

    public IReadOnlyList<RuntimeSample> CurrentFleet()
    {
        return _byKey.Values
            .Select(e => e.Latest)
            .Where(s => s is not null)
            .Select(s => s!)
            .OrderBy(s => s.Component, StringComparer.Ordinal)
            .ThenBy(s => s.Pod ?? s.ScrapedFromIp ?? "", StringComparer.Ordinal)
            .ToList();
    }

    public IReadOnlyList<RuntimeSample> History(string component, string podOrIp, int max)
    {
        var key = $"{component}|{podOrIp}";
        if (!_byKey.TryGetValue(key, out var e)) return Array.Empty<RuntimeSample>();
        return e.Snapshot(max);
    }

    /// <summary>
    /// Remove entries that haven't been refreshed for longer than
    /// <paramref name="maxAge"/>. The scraper calls this once per cycle so
    /// rolled / re-scheduled pods (which get fresh names) don't accumulate
    /// forever in the dashboard.
    /// </summary>
    public int EvictStale(TimeSpan maxAge)
    {
        var cutoff = DateTime.UtcNow - maxAge;
        int dropped = 0;
        foreach (var (key, entry) in _byKey)
        {
            var latest = entry.Latest;
            // An entry without a Latest sample was created speculatively (e.g.
            // a DNS-failure MarkError) and never updated.  Treat that exactly
            // the same as a stale one.
            var lastSeen = latest?.UpdatedUtc ?? DateTime.MinValue;
            if (lastSeen < cutoff && _byKey.TryRemove(key, out _))
                dropped++;
        }
        return dropped;
    }

    /// <summary>
    /// Limit retention to the set of (component, key) pairs we just observed
    /// this cycle.  Anything not present in <paramref name="alive"/> AND older
    /// than <paramref name="graceAge"/> is dropped immediately.  We keep the
    /// grace age so a single missed scrape doesn't flicker a healthy pod off
    /// the dashboard.
    /// </summary>
    public int Retain(HashSet<string> aliveKeys, TimeSpan graceAge)
    {
        var cutoff = DateTime.UtcNow - graceAge;
        int dropped = 0;
        foreach (var (key, entry) in _byKey)
        {
            if (aliveKeys.Contains(key)) continue;
            var latest = entry.Latest;
            var lastSeen = latest?.UpdatedUtc ?? DateTime.MinValue;
            if (lastSeen < cutoff && _byKey.TryRemove(key, out _))
                dropped++;
        }
        return dropped;
    }

    /// <summary>Stable lookup key used in storage (component|pod-or-ip).</summary>
    internal static string BuildKey(string component, string podOrIp) =>
        $"{component}|{podOrIp}";

    private sealed class PodEntry
    {
        private readonly object _lock = new();
        private readonly RuntimeSample?[] _ring = new RuntimeSample?[HistoryDepth];
        private long _seq;
        private RuntimeSample? _latest;

        public RuntimeSample? Latest
        {
            get { lock (_lock) return _latest; }
        }

        public void Apply(RuntimeSample s)
        {
            lock (_lock)
            {
                _latest = s;
                _ring[(int)(_seq % HistoryDepth)] = s;
                _seq++;
            }
        }

        public void MarkError(string component, string ip, string? pod, string error)
        {
            lock (_lock)
            {
                // Keep the most recent successful sample as Latest, but augment with the
                // error so the dashboard can flag staleness. If we've never had a good
                // sample, surface a placeholder so the row still shows up.
                if (_latest is null)
                {
                    _latest = new RuntimeSample
                    {
                        Component = component,
                        Pod = pod,
                        ScrapedFromIp = ip,
                        UpdatedUtc = DateTime.UtcNow,
                        Error = error,
                    };
                }
                else
                {
                    _latest = _latest with { Error = error, UpdatedUtc = DateTime.UtcNow };
                }
            }
        }

        public IReadOnlyList<RuntimeSample> Snapshot(int max)
        {
            lock (_lock)
            {
                int count = (int)Math.Min(_seq, HistoryDepth);
                int take = Math.Min(max, count);
                var result = new List<RuntimeSample>(take);
                long start = _seq - take;
                for (int i = 0; i < take; i++)
                {
                    var v = _ring[(int)((start + i) % HistoryDepth)];
                    if (v is not null) result.Add(v);
                }
                return result;
            }
        }
    }
}

/// <summary>Wire-shape of a runtime sample. Field names are snake_case to match
/// the JSON the workload services emit. Adding fields is non-breaking (extra
/// fields in the response are tolerated by the deserialiser); renaming or
/// changing the semantic of a field requires updating cross / gateway / agent
/// in lock-step.</summary>
public sealed record RuntimeSample
{
    [JsonPropertyName("ts_unix_ms")]      public long TsUnixMs { get; init; }
    [JsonPropertyName("component")]       public string Component { get; init; } = "";
    [JsonPropertyName("pod")]             public string? Pod { get; init; }
    [JsonPropertyName("node")]            public string? Node { get; init; }
    [JsonPropertyName("process_uptime_sec")] public double ProcessUptimeSec { get; init; }

    [JsonPropertyName("heap_size_bytes")]       public long HeapSizeBytes { get; init; }
    [JsonPropertyName("heap_committed_bytes")]  public long HeapCommittedBytes { get; init; }
    [JsonPropertyName("heap_fragmented_bytes")] public long HeapFragmentedBytes { get; init; }

    [JsonPropertyName("gen0_size")] public long Gen0Size { get; init; }
    [JsonPropertyName("gen1_size")] public long Gen1Size { get; init; }
    [JsonPropertyName("gen2_size")] public long Gen2Size { get; init; }
    [JsonPropertyName("loh_size")]  public long LohSize { get; init; }
    [JsonPropertyName("loh_fragmented_bytes")] public long LohFragmentedBytes { get; init; }
    [JsonPropertyName("poh_size")]  public long PohSize { get; init; }

    [JsonPropertyName("gen0_collections")] public int Gen0Collections { get; init; }
    [JsonPropertyName("gen1_collections")] public int Gen1Collections { get; init; }
    [JsonPropertyName("gen2_collections")] public int Gen2Collections { get; init; }
    [JsonPropertyName("gc_pause_total_sec")] public double GcPauseTotalSec { get; init; }
    [JsonPropertyName("time_in_gc_pct")]   public double TimeInGcPct { get; init; }

    [JsonPropertyName("rss_bytes")]            public long RssBytes { get; init; }
    [JsonPropertyName("private_bytes")]        public long PrivateBytes { get; init; }
    [JsonPropertyName("memory_load_bytes")]    public long MemoryLoadBytes { get; init; }
    [JsonPropertyName("memory_load_threshold")]public long MemoryLoadThreshold { get; init; }
    [JsonPropertyName("native_overhead_bytes")]public long NativeOverheadBytes { get; init; }

    // ── ThreadPool occupancy ─────────────────────────────────────────────
    [JsonPropertyName("threadpool_workers_busy")]     public int ThreadpoolWorkersBusy { get; init; }
    [JsonPropertyName("threadpool_workers_max")]      public int ThreadpoolWorkersMax { get; init; }
    [JsonPropertyName("threadpool_workers_min")]      public int ThreadpoolWorkersMin { get; init; }
    [JsonPropertyName("threadpool_completion_busy")]  public int ThreadpoolCompletionBusy { get; init; }
    [JsonPropertyName("threadpool_completion_max")]   public int ThreadpoolCompletionMax { get; init; }
    [JsonPropertyName("threadpool_completion_min")]   public int ThreadpoolCompletionMin { get; init; }
    [JsonPropertyName("threadpool_queue_length")]     public long ThreadpoolQueueLength { get; init; }

    // ── Open file descriptors (Linux only; 0 elsewhere). Monotonic growth
    //    while the cluster is steady ⇒ FD/socket leak. ───────────────────
    [JsonPropertyName("open_fd_count")] public int OpenFdCount { get; init; }

    // ── gRPC channel cache size (cross + gateway). Monotonic growth ⇒
    //    channel cache leak (e.g. agent IP churn bypassing eviction). ────
    [JsonPropertyName("grpc_channel_cache_count")] public int GrpcChannelCacheCount { get; init; }
    [JsonPropertyName("grpc_client_cache_count")]  public int GrpcClientCacheCount { get; init; }

    // ── Live rolling stage percentiles (cross only). Each entry covers the
    //    last ~5 min for one named stage. Empty / null on other components. ─
    [JsonPropertyName("stage_stats")] public List<StageStatsSample>? StageStats { get; init; }

    // ── Storage cardinality + RocksDB engine stats (agent only). Both
    //    rocksdb_* objects are nullable so non-agent components can omit
    //    them without breaking the deserialiser. ───────────────────────────
    [JsonPropertyName("rocks_unique_chunks")]     public long RocksUniqueChunks { get; init; }
    [JsonPropertyName("rocks_total_chunk_bytes")] public long RocksTotalChunkBytes { get; init; }
    [JsonPropertyName("rocks_total_buckets")]     public long RocksTotalBuckets { get; init; }
    [JsonPropertyName("rocks_total_vectors")]     public long RocksTotalVectors { get; init; }
    [JsonPropertyName("rocksdb_chunk")]           public RocksDbStatsSample? RocksdbChunk { get; init; }
    [JsonPropertyName("rocksdb_bucket")]          public RocksDbStatsSample? RocksdbBucket { get; init; }

    // ── Replication health (Phase 7) ───────────────────────────────────
    // ring_size: how many agents are on the consistent-hash ring right
    //   now (cross/gateway only — agent has no local ring snapshot).
    // topology_version: bumps on every membership delta. Plotted as a
    //   rate so the operator can spot "the cluster is churning".
    // replication_factor / write_quorum: env-driven knobs surfaced
    //   here so the dashboard makes drift between pods obvious (a
    //   half-rolled deployment with two different R values is a real
    //   class of bug we want to catch fast).
    // vnodes_per_agent: applies to cross/gateway (active ring snapshot)
    //   and agent (own self-registration value).
    [JsonPropertyName("ring_size")]               public int RingSize { get; init; }
    [JsonPropertyName("topology_version")]        public long TopologyVersion { get; init; }
    [JsonPropertyName("replication_factor")]     public int ReplicationFactor { get; init; }
    [JsonPropertyName("write_quorum")]           public int WriteQuorum { get; init; }
    [JsonPropertyName("vnodes_per_agent")]       public int VnodesPerAgent { get; init; }
    [JsonPropertyName("rebalance_coordinator_enabled")] public bool RebalanceCoordinatorEnabled { get; init; }
    [JsonPropertyName("anti_entropy_enabled")]   public bool AntiEntropyEnabled { get; init; }
    [JsonPropertyName("anti_entropy_auto_repair")] public bool AntiEntropyAutoRepair { get; init; }

    // Set server-side after deserialisation; not part of the wire response from workloads.
    [JsonIgnore] public string? ScrapedFromIp { get; init; }
    [JsonIgnore] public DateTime UpdatedUtc { get; init; }
    [JsonIgnore] public string? Error { get; init; }
}

/// <summary>Per-stage rolling percentile sample (last ~5 min).</summary>
public sealed record StageStatsSample
{
    [JsonPropertyName("stage")]        public string Stage { get; init; } = "";
    [JsonPropertyName("count_window")] public int CountWindow { get; init; }
    [JsonPropertyName("total_count")]  public long TotalCount { get; init; }
    [JsonPropertyName("min_ms")]       public double MinMs { get; init; }
    [JsonPropertyName("p50_ms")]       public double P50Ms { get; init; }
    [JsonPropertyName("p95_ms")]       public double P95Ms { get; init; }
    [JsonPropertyName("p99_ms")]       public double P99Ms { get; init; }
    [JsonPropertyName("max_ms")]       public double MaxMs { get; init; }
}

/// <summary>RocksDB per-DB engine stats; two of these per agent (chunk + bucket).</summary>
public sealed record RocksDbStatsSample
{
    [JsonPropertyName("total_sst_bytes")]          public long TotalSstBytes { get; init; }
    [JsonPropertyName("live_sst_bytes")]           public long LiveSstBytes { get; init; }
    [JsonPropertyName("pending_compaction_bytes")] public long PendingCompactionBytes { get; init; }
    [JsonPropertyName("compaction_pending")]       public bool CompactionPending { get; init; }
    [JsonPropertyName("write_stopped")]            public bool WriteStopped { get; init; }
    [JsonPropertyName("memtable_bytes")]           public long MemtableBytes { get; init; }
    [JsonPropertyName("immutable_memtable_count")] public int ImmutableMemtableCount { get; init; }
    [JsonPropertyName("block_cache_used_bytes")]   public long BlockCacheUsedBytes { get; init; }
    [JsonPropertyName("block_cache_pinned_bytes")] public long BlockCachePinnedBytes { get; init; }
    [JsonPropertyName("table_reader_mem_bytes")]   public long TableReaderMemBytes { get; init; }
    [JsonPropertyName("estimate_num_keys")]        public long EstimateNumKeys { get; init; }
    [JsonPropertyName("num_files_per_level")]      public int[] NumFilesPerLevel { get; init; } = Array.Empty<int>();
    [JsonPropertyName("actual_delayed_write_rate")] public long ActualDelayedWriteRate { get; init; }
}
