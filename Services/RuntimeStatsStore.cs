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

    public void UpdateOk(string component, string ip, RuntimeSample sample)
    {
        var key = $"{component}|{sample.Pod ?? ip}";
        var entry = _byKey.GetOrAdd(key, _ => new PodEntry());
        entry.Apply(sample with
        {
            Component = component,
            ScrapedFromIp = ip,
            UpdatedUtc = DateTime.UtcNow,
            Error = null,
        });
    }

    public void MarkError(string component, string ip, string? pod, string error)
    {
        var key = $"{component}|{pod ?? ip}";
        var entry = _byKey.GetOrAdd(key, _ => new PodEntry());
        entry.MarkError(component, ip, pod, error);
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

/// <summary>Wire-shape of a runtime sample. Field names are snake_case to match the
/// JSON the workload services emit.</summary>
public sealed record RuntimeSample
{
    [JsonPropertyName("ts_unix_ms")]      public long TsUnixMs { get; init; }
    [JsonPropertyName("component")]       public string Component { get; init; } = "";
    [JsonPropertyName("pod")]             public string? Pod { get; init; }
    [JsonPropertyName("node")]            public string? Node { get; init; }

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

    [JsonPropertyName("rss_bytes")]            public long RssBytes { get; init; }
    [JsonPropertyName("private_bytes")]        public long PrivateBytes { get; init; }
    [JsonPropertyName("memory_load_bytes")]    public long MemoryLoadBytes { get; init; }
    [JsonPropertyName("memory_load_threshold")]public long MemoryLoadThreshold { get; init; }
    [JsonPropertyName("native_overhead_bytes")]public long NativeOverheadBytes { get; init; }

    // Set server-side after deserialisation; not part of the wire response from workloads.
    [JsonIgnore] public string? ScrapedFromIp { get; init; }
    [JsonIgnore] public DateTime UpdatedUtc { get; init; }
    [JsonIgnore] public string? Error { get; init; }
}
