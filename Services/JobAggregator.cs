using System.Collections.Concurrent;
using Crossv9.Jobevents;

namespace Controlcenter.Services;

/// <summary>
/// In-memory rolling KPIs for the dashboard top bar. Pure counters; no per-job
/// retention beyond what's needed to compute "active jobs" (the in-flight set).
///
/// Memory bound: O(active jobs) — rows are removed from the in-flight set on
/// COMPLETED/FAILED. If a JOB_STARTED is received but neither COMPLETED nor FAILED
/// is ever observed (cross pod crashed mid-job), the entry will linger; we sweep
/// stale in-flight rows older than 1 hour every minute.
/// </summary>
public sealed class JobAggregator
{
    private long _totalStarted;
    private long _totalCompleted;
    private long _totalFailed;
    private long _totalBlocks;
    private long _totalOriginalBytes;
    private long _totalCompressedBytes;
    private long _totalDcBytes;
    private long _totalRefsFound;
    private long _totalChunks;
    private long _totalServerMs;

    private readonly ConcurrentDictionary<string, ActiveJob> _active = new();

    public sealed record ActiveJob(
        string JobId, string FileName, ulong OriginalSize, JobMode Mode,
        long StartedTsNs, string CrossPod,
        long BlocksDone, long BlockCount, long BytesIn, long BytesOut,
        long RefsFound, long Chunks, long DcBytes, string LastStage);

    public void Apply(JobEvent ev)
    {
        switch (ev.Phase)
        {
            case JobPhase.Started:
                Interlocked.Increment(ref _totalStarted);
                _active[ev.JobId] = new ActiveJob(
                    ev.JobId, ev.FileName, ev.OriginalSize, ev.Mode,
                    (long)ev.TsUnixNs, ev.CrossPod, 0, 0, 0, 0, 0, 0, 0, "");
                break;

            case JobPhase.StageDone:
                if (_active.TryGetValue(ev.JobId, out var aStage))
                    _active[ev.JobId] = aStage with { LastStage = ev.Stage };
                break;

            case JobPhase.BlockDone:
                Interlocked.Increment(ref _totalBlocks);
                if (!string.IsNullOrEmpty(ev.ParentJobId) &&
                    _active.TryGetValue(ev.ParentJobId, out var aBlock))
                {
                    _active[ev.ParentJobId] = aBlock with
                    {
                        BlocksDone = aBlock.BlocksDone + 1,
                        BlockCount = ev.BlockCount,
                        BytesIn = aBlock.BytesIn + (long)ev.BlockBytesIn,
                        BytesOut = aBlock.BytesOut + (long)ev.BlockBytesOut,
                        RefsFound = aBlock.RefsFound + ev.BlockRefsFound,
                        Chunks = aBlock.Chunks + ev.BlockChunks,
                        DcBytes = aBlock.DcBytes + (long)ev.BlockDcBytes,
                    };
                }
                break;

            case JobPhase.Completed:
                Interlocked.Increment(ref _totalCompleted);
                Interlocked.Add(ref _totalCompressedBytes, (long)ev.FinalCompressedSize);
                Interlocked.Add(ref _totalDcBytes, (long)ev.FinalDcBytes);
                Interlocked.Add(ref _totalRefsFound, ev.FinalRefsFound);
                Interlocked.Add(ref _totalChunks, ev.FinalChunks);
                Interlocked.Add(ref _totalServerMs, (long)ev.FinalServerMs);
                if (_active.TryRemove(ev.JobId, out var doneJob))
                    Interlocked.Add(ref _totalOriginalBytes, (long)doneJob.OriginalSize);
                break;

            case JobPhase.Failed:
                Interlocked.Increment(ref _totalFailed);
                _active.TryRemove(ev.JobId, out _);
                break;
        }

        // Cheap stale sweep: skip when active set is small.
        if (_active.Count > 256 && (DateTime.UtcNow.Ticks % 1024) == 0)
            SweepStale();
    }

    private void SweepStale()
    {
        long cutoff = (DateTime.UtcNow - TimeSpan.FromHours(1) - DateTime.UnixEpoch).Ticks * 100;
        foreach (var kv in _active)
        {
            if (kv.Value.StartedTsNs < cutoff)
                _active.TryRemove(kv.Key, out _);
        }
    }

    public Snapshot CurrentSnapshot()
    {
        var active = _active.Values.ToArray();
        return new Snapshot(
            TotalStarted: Interlocked.Read(ref _totalStarted),
            TotalCompleted: Interlocked.Read(ref _totalCompleted),
            TotalFailed: Interlocked.Read(ref _totalFailed),
            TotalBlocks: Interlocked.Read(ref _totalBlocks),
            TotalOriginalBytes: Interlocked.Read(ref _totalOriginalBytes),
            TotalCompressedBytes: Interlocked.Read(ref _totalCompressedBytes),
            TotalDcBytes: Interlocked.Read(ref _totalDcBytes),
            TotalRefsFound: Interlocked.Read(ref _totalRefsFound),
            TotalChunks: Interlocked.Read(ref _totalChunks),
            TotalServerMs: Interlocked.Read(ref _totalServerMs),
            ActiveJobs: active);
    }

    public sealed record Snapshot(
        long TotalStarted,
        long TotalCompleted,
        long TotalFailed,
        long TotalBlocks,
        long TotalOriginalBytes,
        long TotalCompressedBytes,
        long TotalDcBytes,
        long TotalRefsFound,
        long TotalChunks,
        long TotalServerMs,
        ActiveJob[] ActiveJobs);
}
