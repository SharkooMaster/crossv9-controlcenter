namespace Controlcenter.Services;

/// <summary>
/// Periodically snapshots the cumulative datacenter totals — bytes ingested,
/// bytes physically stored on the DC, and bytes returned to customers (CCF) —
/// into a bounded time series. This is the instrument for the central economic
/// question of the universal-datacenter model: does physical storage growth
/// <em>flatten</em> as more (similar) data is ingested? If the global dictionary
/// saturates, the "stored" line bends away from the "ingested" line and the
/// marginal store rate (Δstored ÷ Δingested) trends toward zero.
///
/// Samples are coalesced while idle: if the cumulative totals haven't moved since
/// the last sample, the most recent point's timestamp is slid forward instead of
/// appending a flat run, so the ring stays focused on active periods.
///
/// Memory bound: capacity × ~48 B. Default 4096 → ~200 KB.
/// </summary>
public sealed class SaturationSampler : BackgroundService
{
    public readonly record struct Sample(
        long TsMs, long BytesIn, long BytesDc, long BytesOut, long Chunks, long Refs);

    private readonly JobAggregator _agg;
    private readonly object _lock = new();
    private readonly Sample[] _buf;
    private readonly int _capacity;
    private readonly int _intervalSec;
    private long _count;   // total appended (monotonic sequence)
    private int _head;     // next write index

    public SaturationSampler(JobAggregator agg)
    {
        _agg = agg;
        _capacity = ParseInt("SATURATION_HISTORY_CAP", 4096, 16);
        _intervalSec = ParseInt("SATURATION_SAMPLE_INTERVAL_SEC", 5, 1);
        _buf = new Sample[_capacity];
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Seed one point immediately so the chart isn't empty on first open.
        RecordOnce();
        var delay = TimeSpan.FromSeconds(_intervalSec);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(delay, stoppingToken);
                RecordOnce();
            }
        }
        catch (OperationCanceledException) { /* shutting down */ }
    }

    private void RecordOnce()
    {
        var s = _agg.CurrentSnapshot();

        // Fold in live in-flight progress. The cumulative job counters only
        // advance on COMPLETED, so a single large file (one long-running job
        // that streams thousands of blocks) would otherwise leave the series
        // flat at zero until the very end. Each active job accumulates per-block
        // BytesIn / DcBytes / BytesOut, so adding those makes the curve fill in
        // continuously during the run. There's a tiny discontinuity at the
        // moment a job completes (its block-summed BytesIn is swapped for the
        // exact OriginalSize), but it's negligible for a saturation trend.
        long liveIn = s.TotalOriginalBytes;
        long liveDc = s.TotalDcBytes;
        long liveOut = s.TotalCompressedBytes;
        long chunks = s.TotalChunks;
        long refs = s.TotalRefsFound;
        foreach (var j in s.ActiveJobs)
        {
            liveIn += j.BytesIn;
            liveDc += j.DcBytes;
            liveOut += j.BytesOut;
            chunks += j.Chunks;
            refs += j.RefsFound;
        }

        var sample = new Sample(
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            liveIn, liveDc, liveOut, chunks, refs);

        lock (_lock)
        {
            if (_count > 0)
            {
                int lastIdx = (_head - 1 + _capacity) % _capacity;
                var last = _buf[lastIdx];
                // Idle coalesce: same byte totals → just refresh the timestamp.
                if (last.BytesIn == sample.BytesIn &&
                    last.BytesDc == sample.BytesDc &&
                    last.BytesOut == sample.BytesOut)
                {
                    _buf[lastIdx] = sample;
                    return;
                }
            }
            _buf[_head] = sample;
            _head = (_head + 1) % _capacity;
            _count++;
        }
    }

    /// <summary>Most recent <paramref name="max"/> samples, oldest first.</summary>
    public List<Sample> Snapshot(int max)
    {
        lock (_lock)
        {
            int n = (int)Math.Min(_count, _capacity);
            int take = Math.Min(max, n);
            var result = new List<Sample>(take);
            long start = _count - take;
            for (int i = 0; i < take; i++)
            {
                int idx = (int)((start + i) % _capacity);
                result.Add(_buf[idx]);
            }
            return result;
        }
    }

    /// <summary>Clears the series. Wired to POST /api/reset alongside the KPI counters.</summary>
    public void Reset()
    {
        lock (_lock)
        {
            _count = 0;
            _head = 0;
            Array.Clear(_buf, 0, _buf.Length);
        }
    }

    private static int ParseInt(string env, int dflt, int min)
    {
        var raw = Environment.GetEnvironmentVariable(env);
        return int.TryParse(raw, out var v) && v >= min ? v : dflt;
    }
}
