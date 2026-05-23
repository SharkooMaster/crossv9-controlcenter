namespace Controlcenter.Services;

/// <summary>
/// Periodically snapshots <see cref="RuntimeStatsStore.CurrentFleet"/> and
/// forwards each pod's latest sample into <see cref="JournalWriter"/> so that
/// runtime telemetry (heap, GC %, threadpool, FD count, RocksDB compaction
/// state, stage percentiles) ends up in the same downloadable daily gzip
/// NDJSON file as the job-event tape.
///
/// Why a separate background service rather than emitting from the scraper:
///   - The scraper's job is to keep the live in-memory store fresh on a tight
///     ~30 s cadence so the dashboard graph is responsive. We don't want to
///     persist every single scrape — that's an order of magnitude more lines
///     per day than necessary, and a sustained scrape spike would amplify
///     directly into journal pressure.
///   - This journaler runs on its own cadence (default 60 s) so the on-disk
///     trail is regular even when scrapes are flaky, and decoupled enough
///     that a slow disk doesn't backpressure the scraper.
///
/// Memory budget: zero allocations beyond the bounded list returned by
/// <c>CurrentFleet()</c>; the journal channel applies the same DropWrite back
/// pressure as job events, so a stalled disk loses perf samples (telemetry is
/// best-effort) instead of growing memory unbounded.
/// </summary>
public sealed class FleetSampleJournaler : BackgroundService
{
    private readonly RuntimeStatsStore _store;
    private readonly JournalWriter _journal;
    private readonly TimeSpan _interval;

    public FleetSampleJournaler(RuntimeStatsStore store, JournalWriter journal)
    {
        _store = store;
        _journal = journal;

        // The interval defaults to 60 s independently of the scraper cadence.
        // At 5 components × 3 pods × 60 s = ~3.6 KB/min compressed → ~5 MB/day,
        // negligible against the 7-day journal retention budget.
        _interval = TimeSpan.FromSeconds(
            int.TryParse(Environment.GetEnvironmentVariable("FLEET_JOURNAL_INTERVAL_SEC"), out var v) && v > 0
                ? v
                : 60);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine($"[FleetJournaler] Starting; interval={_interval.TotalSeconds:F0}s");

        // Tiny initial delay so the scraper has had at least one cycle to
        // populate the store before we record the first batch — avoids a
        // dozen "Error" / placeholder rows showing up in the journal as the
        // very first datapoint after pod boot.
        try { await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken); }
        catch (OperationCanceledException) { return; }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var fleet = _store.CurrentFleet();
                int written = 0;
                int dropped = 0;
                foreach (var s in fleet)
                {
                    if (_journal.TryEnqueuePerfSample(s)) written++;
                    else dropped++;
                }
                if (dropped > 0)
                    Console.WriteLine($"[FleetJournaler] Snapshot: wrote={written}, dropped={dropped} (journal channel full)");
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                Console.WriteLine($"[FleetJournaler] cycle error: {ex.GetType().Name}: {ex.Message}");
            }

            try { await Task.Delay(_interval, stoppingToken); }
            catch (OperationCanceledException) { return; }
        }
    }
}
