using System.Net;
using System.Text.Json;

namespace Controlcenter.Services;

/// <summary>
/// Periodically resolves each headless-service DNS name listed in
/// <c>FLEET_TARGETS</c>, hits every resolved pod IP at <c>:5001/stats/runtime</c>,
/// and writes the result into <see cref="RuntimeStatsStore"/>. Single background
/// thread, fan-out per cycle.
///
/// Pull-based by design: if this service is paused / restarted / crashed, the
/// workload pods don't notice — they only respond to incoming HTTP, they don't
/// push anything. This is the lowest-impact way to observe their fragmentation.
///
/// Configuration:
///   <c>FLEET_TARGETS</c>: comma-separated list of <c>component=hostname</c>
///       entries. Example:
///         agent=crossv9-crossv9-agent-headless,cross=crossv9-crossv9-cross-headless,gateway=crossv9-crossv9-gateway-headless
///   <c>FLEET_SCRAPE_INTERVAL_SEC</c>: defaults to 30.
///   <c>FLEET_SCRAPE_TIMEOUT_SEC</c>:  defaults to 5.
///   <c>FLEET_SCRAPE_PORT</c>:         defaults to 5001.
/// </summary>
public sealed class RuntimeStatsScraper : BackgroundService
{
    private readonly RuntimeStatsStore _store;
    private readonly HttpClient _http;
    private readonly TimeSpan _interval;
    private readonly int _port;
    private readonly (string component, string host)[] _targets;

    private static readonly JsonSerializerOptions _jsonOpts = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    public RuntimeStatsScraper(RuntimeStatsStore store)
    {
        _store = store;

        var timeout = TimeSpan.FromSeconds(
            int.TryParse(Environment.GetEnvironmentVariable("FLEET_SCRAPE_TIMEOUT_SEC"), out var t) && t > 0 ? t : 5);
        _http = new HttpClient(new SocketsHttpHandler
        {
            ConnectTimeout = TimeSpan.FromSeconds(3),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
            MaxConnectionsPerServer = 4,
        })
        {
            Timeout = timeout,
        };

        _interval = TimeSpan.FromSeconds(
            int.TryParse(Environment.GetEnvironmentVariable("FLEET_SCRAPE_INTERVAL_SEC"), out var i) && i > 0 ? i : 30);

        _port = int.TryParse(Environment.GetEnvironmentVariable("FLEET_SCRAPE_PORT"), out var p) && p > 0 ? p : 5001;

        var raw = Environment.GetEnvironmentVariable("FLEET_TARGETS") ?? "";
        _targets = raw
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(ParseTarget)
            .Where(t => t.component is not null && t.host is not null)
            .Select(t => (t.component!, t.host!))
            .ToArray();
    }

    private static (string? component, string? host) ParseTarget(string raw)
    {
        var eq = raw.IndexOf('=');
        if (eq <= 0) return (null, null);
        var component = raw[..eq].Trim();
        var host = raw[(eq + 1)..].Trim();
        return (component.Length == 0 || host.Length == 0) ? (null, null) : (component, host);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_targets.Length == 0)
        {
            Console.WriteLine("[FleetScraper] FLEET_TARGETS empty; scraper idle.");
            return;
        }

        // Grace window before a pod that wasn't seen this cycle is evicted.
        // Default = 3 × interval; minimum of 90s. Tuneable via env var so the
        // operator can hide flapping pods longer if the cluster's DNS lags.
        var graceSecRaw = Environment.GetEnvironmentVariable("FLEET_EVICT_GRACE_SEC");
        var grace = int.TryParse(graceSecRaw, out var g) && g > 0
            ? TimeSpan.FromSeconds(g)
            : TimeSpan.FromSeconds(Math.Max(90, (int)_interval.TotalSeconds * 3));

        Console.WriteLine($"[FleetScraper] Targeting {_targets.Length} component(s): " +
            string.Join(", ", _targets.Select(t => $"{t.component}={t.host}")) +
            $"; interval={_interval.TotalSeconds:F0}s, evictGrace={grace.TotalSeconds:F0}s");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var aliveKeys = await ScrapeAllAsync(stoppingToken);
                // Drop entries we didn't refresh this cycle that have also
                // been stale longer than the grace window. New pod names from
                // a rolling restart land in aliveKeys this cycle; the old
                // pod's entry will fall out one grace window later.
                int dropped = _store.Retain(aliveKeys, grace);
                if (dropped > 0)
                    Console.WriteLine($"[FleetScraper] Evicted {dropped} stale pod(s) (alive={aliveKeys.Count}, grace={grace.TotalSeconds:F0}s)");
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                Console.WriteLine($"[FleetScraper] cycle error: {ex.GetType().Name}: {ex.Message}");
            }

            try { await Task.Delay(_interval, stoppingToken); }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task<HashSet<string>> ScrapeAllAsync(CancellationToken ct)
    {
        // Each pod we touch this cycle records the store key it landed on so
        // Retain() can decide what's still live versus what dropped out
        // between cycles (rolling restart, pod evicted, node drained, etc.).
        var alive = new HashSet<string>(StringComparer.Ordinal);
        var aliveLock = new object();
        void RecordAlive(string key)
        {
            lock (aliveLock) alive.Add(key);
        }

        var tasks = new List<Task>(_targets.Length * 2);
        foreach (var (component, host) in _targets)
        {
            IPAddress[] ips;
            try
            {
                ips = await Dns.GetHostAddressesAsync(host, ct);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[FleetScraper] DNS resolve failed for {host}: {ex.Message}");
                // DNS failure is a transient component-level error; key it on
                // the hostname so the dashboard surfaces ONE row, not one per
                // historic IP, and so the entry expires naturally with the
                // grace window if DNS stays broken.
                RecordAlive(_store.MarkError(component, host, null, $"dns: {ex.GetType().Name}"));
                continue;
            }

            if (ips.Length == 0)
            {
                RecordAlive(_store.MarkError(component, host, null, "dns: no records"));
                continue;
            }

            foreach (var ip in ips)
            {
                tasks.Add(ScrapeOneAsync(component, ip.ToString(), RecordAlive, ct));
            }
        }

        await Task.WhenAll(tasks);
        return alive;
    }

    private async Task ScrapeOneAsync(string component, string ip, Action<string> recordAlive, CancellationToken ct)
    {
        var url = $"http://{ip}:{_port}/stats/runtime";
        try
        {
            using var resp = await _http.GetAsync(url, HttpCompletionOption.ResponseContentRead, ct);
            if (!resp.IsSuccessStatusCode)
            {
                recordAlive(_store.MarkError(component, ip, null, $"http {(int)resp.StatusCode}"));
                return;
            }
            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            var sample = await JsonSerializer.DeserializeAsync<RuntimeSample>(stream, _jsonOpts, ct);
            if (sample is null)
            {
                recordAlive(_store.MarkError(component, ip, null, "deserialise: null"));
                return;
            }
            recordAlive(_store.UpdateOk(component, ip, sample));
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { /* shutting down */ }
        catch (TaskCanceledException) { recordAlive(_store.MarkError(component, ip, null, "timeout")); }
        catch (HttpRequestException ex) { recordAlive(_store.MarkError(component, ip, null, $"http: {ex.Message}")); }
        catch (Exception ex) { recordAlive(_store.MarkError(component, ip, null, $"{ex.GetType().Name}: {ex.Message}")); }
    }

    public override void Dispose()
    {
        try { _http.Dispose(); } catch { }
        base.Dispose();
    }
}
