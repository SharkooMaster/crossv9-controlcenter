using System.Collections.Concurrent;
using Crossv9.Jobevents;

namespace Controlcenter.Services;

/// <summary>
/// Maintains a live topology of the request graph: which pods are talking to which, with
/// rolling per-edge stats. Fed by <see cref="JobEventReceiverService"/> at every JobEvent
/// arrival; plays back the structural model:
///
///   client → cross[pod]                 on Started
///   cross[pod] → gateway-aggregate      on StageDone for stages that fan out to gateway
///   gateway-aggregate → agent[pod]      derived (1 pulse per known agent on the same fan-out)
///   cross[pod] → client                 on Completed
///
/// Edges accumulate (count, bytes, last_ts_ns, last_stage). Topology snapshot is computed
/// on demand and merged with <see cref="RuntimeStatsStore"/> for the node list. The model
/// is intentionally synthetic: we don't (yet) instrument gateway↔agent RPCs, so that lane
/// is *inferred* from the cross-side stage events. It's an honest upper-bound visualization
/// of where bytes flowed, not a packet trace.
/// </summary>
public sealed class TopologyTracker
{
    public const string ClientNodeId = "_client";
    public const string GatewayAggregateId = "_gateway";

    // Stages that, on the cross side, imply a network call out to the gateway → agent
    // lane. Every other stage is local cross work and shouldn't show as edge traffic.
    private static readonly HashSet<string> GatewayBoundStages = new(StringComparer.Ordinal)
    {
        "SearchBuckets",
        "StoreChunks",
        "BatchGet",
        "BatchStore",
    };

    private readonly RuntimeStatsStore _stats;
    private readonly ConcurrentDictionary<EdgeKey, EdgeAccumulator> _edges = new();
    private long _eventsApplied;

    public TopologyTracker(RuntimeStatsStore stats)
    {
        _stats = stats;
    }

    public long EventsApplied => Interlocked.Read(ref _eventsApplied);

    /// <summary>
    /// Reset all accumulated edge stats. Called by /api/reset so the topology view
    /// can be cleared independently of the runtime stats store.
    /// </summary>
    public void Reset()
    {
        _edges.Clear();
        Interlocked.Exchange(ref _eventsApplied, 0);
    }

    public void Apply(JobEvent ev)
    {
        if (string.IsNullOrEmpty(ev.CrossPod)) return;
        Interlocked.Increment(ref _eventsApplied);

        var bytes = ev.StageAttrBytes != 0 ? ev.StageAttrBytes : ev.OriginalSize;

        switch (ev.Phase)
        {
            case JobPhase.Started:
                Bump(ClientNodeId, ev.CrossPod, ev.TsUnixNs, "Started", ev.OriginalSize);
                break;

            case JobPhase.StageDone:
                if (!string.IsNullOrEmpty(ev.Stage) && GatewayBoundStages.Contains(ev.Stage))
                {
                    Bump(ev.CrossPod, GatewayAggregateId, ev.TsUnixNs, ev.Stage, bytes);
                    // Fan-out to known agents — bytes split by agent count so the
                    // aggregate edge accounting stays roughly balanced.
                    var fleet = _stats.CurrentFleet();
                    var agents = fleet
                        .Where(s => string.Equals(s.Component, "agent", StringComparison.Ordinal)
                                 && !string.IsNullOrEmpty(s.Pod))
                        .Select(s => s.Pod!)
                        .ToList();
                    if (agents.Count > 0)
                    {
                        var per = bytes / (ulong)agents.Count;
                        foreach (var a in agents)
                            Bump(GatewayAggregateId, a, ev.TsUnixNs, ev.Stage, per);
                    }
                }
                break;

            case JobPhase.Completed:
                Bump(ev.CrossPod, ClientNodeId, ev.TsUnixNs, "Completed", ev.FinalCompressedSize);
                break;
        }
    }

    private void Bump(string src, string dst, ulong tsNs, string stage, ulong bytes)
    {
        var key = new EdgeKey(src, dst);
        var edge = _edges.GetOrAdd(key, _ => new EdgeAccumulator());
        edge.Add(tsNs, stage, bytes);
    }

    public TopologySnapshot Snapshot()
    {
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var fleet = _stats.CurrentFleet();

        var nodes = new List<TopologyNode>();
        nodes.Add(new TopologyNode(ClientNodeId, "client", "client", null, true, 0, 0, 0));
        nodes.Add(new TopologyNode(GatewayAggregateId, "gateway-router", "router", null, true, 0, 0, 0));

        foreach (var s in fleet)
        {
            var ageSec = (DateTime.UtcNow - s.UpdatedUtc).TotalSeconds;
            var alive = ageSec < 90 && string.IsNullOrEmpty(s.Error);
            // Pod-name fallback: if the scraper only knows the IP, use it as id.
            var id = !string.IsNullOrEmpty(s.Pod) ? s.Pod! : (s.ScrapedFromIp ?? "");
            if (string.IsNullOrEmpty(id)) continue;
            nodes.Add(new TopologyNode(
                Id: id,
                Component: s.Component ?? "",
                ShortLabel: ShortLabel(s.Pod, s.ScrapedFromIp, s.Component),
                Node: s.Node,
                Alive: alive,
                HeapBytes: s.HeapSizeBytes,
                RssBytes: s.RssBytes,
                GcPct: s.TimeInGcPct
            ));
        }

        var edges = new List<TopologyEdge>(_edges.Count);
        foreach (var kv in _edges)
        {
            var e = kv.Value.Snapshot();
            edges.Add(new TopologyEdge(
                Src: kv.Key.Src,
                Dst: kv.Key.Dst,
                Count: e.Count,
                Bytes: e.Bytes,
                LastTsNs: e.LastTsNs,
                LastStage: e.LastStage
            ));
        }

        return new TopologySnapshot(nowMs, nodes, edges, EventsApplied);
    }

    private static string ShortLabel(string? pod, string? ip, string? component)
    {
        if (!string.IsNullOrEmpty(pod))
        {
            // Try to pick the trailing replica suffix. For StatefulSet (agent-N) keep
            // the last segment; for Deployment (cross-7777b7656f-dr8kp) keep just the
            // hash tail. Either way we never want to render the noisy chart-prefix.
            var parts = pod.Split('-');
            if (parts.Length > 0)
            {
                var last = parts[^1];
                if (last.Length <= 6) return last;
                return last.Substring(0, 5);
            }
            return pod;
        }
        if (!string.IsNullOrEmpty(ip)) return ip;
        return component ?? "?";
    }

    public readonly record struct EdgeKey(string Src, string Dst);

    private sealed class EdgeAccumulator
    {
        private long _count;
        private long _bytes;
        private long _lastTsNs;
        private string _lastStage = "";
        private readonly object _lock = new();

        public void Add(ulong tsNs, string stage, ulong bytes)
        {
            lock (_lock)
            {
                _count += 1;
                _bytes += unchecked((long)bytes);
                if ((long)tsNs > _lastTsNs)
                {
                    _lastTsNs = (long)tsNs;
                    _lastStage = stage;
                }
            }
        }

        public EdgeStats Snapshot()
        {
            lock (_lock)
            {
                return new EdgeStats(_count, _bytes, _lastTsNs, _lastStage);
            }
        }
    }

    public readonly record struct EdgeStats(long Count, long Bytes, long LastTsNs, string LastStage);
}

public sealed record TopologyNode(
    string Id,
    string Component,
    string ShortLabel,
    string? Node,
    bool Alive,
    long HeapBytes,
    long RssBytes,
    double GcPct
);

public sealed record TopologyEdge(
    string Src,
    string Dst,
    long Count,
    long Bytes,
    long LastTsNs,
    string LastStage
);

public sealed record TopologySnapshot(
    long NowUnixMs,
    IReadOnlyList<TopologyNode> Nodes,
    IReadOnlyList<TopologyEdge> Edges,
    long EventsApplied
);
