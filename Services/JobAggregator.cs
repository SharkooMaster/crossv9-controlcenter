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

    // Integrity diagnostics:
    //   _integrityChecked    — total (chunk,storageGuid) pairs the cross pods verified
    //   _integrityMismatches — pairs where SHA256(chunkBytes) != storageGuid
    //   _integrityDecompressFailures — count of FAILED events with error_class
    //     "INTEGRITY_DECOMPRESS" since the controlcenter started
    //   _refRoundTripChecked / Mismatches / FetchFailures — RefRoundTrip is the
    //     decompress-simulating integrity check (cross fetches each dedup-hit
    //     ref via (BucketId, BucketKey) and compares the agent's response to
    //     the bytes it diffed against). A mismatch here is the canonical
    //     "your decompress will fail" signal and is tracked separately so
    //     it's not lost among the hash-vs-guid round-trip counters.
    private long _integrityChecked;
    private long _integrityMismatches;
    private long _integrityDecompressFailures;
    private long _integrityLastTsNs;
    private string? _integrityLastDetail;

    private long _refRoundTripChecked;
    private long _refRoundTripMismatches;
    private long _refRoundTripFetchFailures;
    private long _refRoundTripLastTsNs;
    private string? _refRoundTripLastDetail;

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

                // Integrity diagnostics piggy-back on StageDone so they don't
                // pollute the failed-jobs counter when the run is otherwise
                // healthy. Stage names start with "IntegrityCheck:" and reuse
                // stage_attr_chunk_count = checked, stage_attr_bucket_count = mismatches.
                if (!string.IsNullOrEmpty(ev.Stage) && ev.Stage.StartsWith("IntegrityCheck:", StringComparison.Ordinal))
                {
                    if (ev.StageAttrChunkCount > 0)
                        Interlocked.Add(ref _integrityChecked, ev.StageAttrChunkCount);
                    if (ev.StageAttrBucketCount > 0)
                    {
                        Interlocked.Add(ref _integrityMismatches, ev.StageAttrBucketCount);
                        Interlocked.Exchange(ref _integrityLastTsNs, (long)ev.TsUnixNs);
                        _integrityLastDetail = ev.Stage;
                    }

                    // Pull RefRoundTrip into its own counters — it's the single
                    // signal that actually predicts decompress failure (the
                    // other stages catch internal bookkeeping bugs but a clean
                    // round-trip across them does not guarantee decompress will
                    // succeed). stage_attr_bytes carries the fetch-failure
                    // sub-count from the emit site.
                    if (ev.Stage == "IntegrityCheck:RefRoundTrip")
                    {
                        if (ev.StageAttrChunkCount > 0)
                            Interlocked.Add(ref _refRoundTripChecked, ev.StageAttrChunkCount);
                        if (ev.StageAttrBucketCount > 0)
                        {
                            Interlocked.Add(ref _refRoundTripMismatches, ev.StageAttrBucketCount);
                            Interlocked.Exchange(ref _refRoundTripLastTsNs, (long)ev.TsUnixNs);
                            _refRoundTripLastDetail =
                                $"{ev.StageAttrBucketCount}/{ev.StageAttrChunkCount} mismatch";
                        }
                        if (ev.StageAttrBytes > 0)
                            Interlocked.Add(ref _refRoundTripFetchFailures, (long)ev.StageAttrBytes);
                    }
                }
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
                if (ev.ErrorClass == "INTEGRITY_DECOMPRESS")
                {
                    Interlocked.Increment(ref _integrityDecompressFailures);
                    Interlocked.Exchange(ref _integrityLastTsNs, (long)ev.TsUnixNs);
                    _integrityLastDetail = $"decompress: {ev.ErrorMessage}";
                }
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

    /// <summary>
    /// Zeroes every cumulative counter and drops the in-flight set. Exposed via
    /// POST /api/reset so the operator can clear the top-bar KPIs (notably the
    /// monotonic "failed" count) between test sessions without having to
    /// restart the controlcenter pod. Does NOT touch the on-disk event journal
    /// or the live SSE tape — those keep their own history independently.
    /// </summary>
    public void ResetAll()
    {
        Interlocked.Exchange(ref _totalStarted, 0);
        Interlocked.Exchange(ref _totalCompleted, 0);
        Interlocked.Exchange(ref _totalFailed, 0);
        Interlocked.Exchange(ref _totalBlocks, 0);
        Interlocked.Exchange(ref _totalOriginalBytes, 0);
        Interlocked.Exchange(ref _totalCompressedBytes, 0);
        Interlocked.Exchange(ref _totalDcBytes, 0);
        Interlocked.Exchange(ref _totalRefsFound, 0);
        Interlocked.Exchange(ref _totalChunks, 0);
        Interlocked.Exchange(ref _totalServerMs, 0);

        Interlocked.Exchange(ref _integrityChecked, 0);
        Interlocked.Exchange(ref _integrityMismatches, 0);
        Interlocked.Exchange(ref _integrityDecompressFailures, 0);
        Interlocked.Exchange(ref _integrityLastTsNs, 0);
        _integrityLastDetail = null;

        Interlocked.Exchange(ref _refRoundTripChecked, 0);
        Interlocked.Exchange(ref _refRoundTripMismatches, 0);
        Interlocked.Exchange(ref _refRoundTripFetchFailures, 0);
        Interlocked.Exchange(ref _refRoundTripLastTsNs, 0);
        _refRoundTripLastDetail = null;

        _active.Clear();
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
            IntegrityChecked: Interlocked.Read(ref _integrityChecked),
            IntegrityMismatches: Interlocked.Read(ref _integrityMismatches),
            IntegrityDecompressFailures: Interlocked.Read(ref _integrityDecompressFailures),
            IntegrityLastTsNs: Interlocked.Read(ref _integrityLastTsNs),
            IntegrityLastDetail: _integrityLastDetail,
            RefRoundTripChecked: Interlocked.Read(ref _refRoundTripChecked),
            RefRoundTripMismatches: Interlocked.Read(ref _refRoundTripMismatches),
            RefRoundTripFetchFailures: Interlocked.Read(ref _refRoundTripFetchFailures),
            RefRoundTripLastTsNs: Interlocked.Read(ref _refRoundTripLastTsNs),
            RefRoundTripLastDetail: _refRoundTripLastDetail,
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
        long IntegrityChecked,
        long IntegrityMismatches,
        long IntegrityDecompressFailures,
        long IntegrityLastTsNs,
        string? IntegrityLastDetail,
        long RefRoundTripChecked,
        long RefRoundTripMismatches,
        long RefRoundTripFetchFailures,
        long RefRoundTripLastTsNs,
        string? RefRoundTripLastDetail,
        ActiveJob[] ActiveJobs);
}
