using System.IO.Compression;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Crossv9.Jobevents;

namespace Controlcenter.Services;

/// <summary>
/// Owns the on-disk event journal. Append-only gzip NDJSON, one file per UTC day.
/// 7-day retention enforced by a periodic sweep.
///
/// Memory budget contract:
///   - One open <see cref="GZipStream"/> + one <see cref="BufferedStream"/> +
///     one <see cref="FileStream"/>. Combined working set ≈ 100 KB.
///   - Bounded inbound channel: <see cref="ChannelCapacity"/> events × ~256 B = ~2.5 MB.
///   - No per-event allocations beyond the JSON byte buffer (reused).
///   - Drops events under sustained pressure rather than buffering unboundedly.
///
/// Concurrency:
///   - Single consumer (<see cref="ExecuteAsync"/>) drains the channel. All file I/O
///     happens here so we never need a lock around the GZipStream.
///   - Producers call <see cref="TryEnqueue"/> which is a single non-blocking TryWrite.
/// </summary>
public sealed class JournalWriter : BackgroundService
{
    public const int ChannelCapacity = 10_000;

    public static readonly TimeSpan RetentionWindow = TimeSpan.FromDays(7);
    public static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(2);
    public static readonly TimeSpan RetentionSweepInterval = TimeSpan.FromHours(1);

    private readonly string _dir;
    private readonly Channel<JobEvent> _channel = Channel.CreateBounded<JobEvent>(
        new BoundedChannelOptions(ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
        });

    private long _enqueued;
    private long _dropped;
    private long _written;
    private long _bytesWritten;

    private DateTime _currentDayUtc = DateTime.MinValue;
    private FileStream? _fs;
    private GZipStream? _gz;
    private BufferedStream? _buf;

    private DateTime _nextSweep = DateTime.MinValue;

    public long Enqueued => Interlocked.Read(ref _enqueued);
    public long Dropped => Interlocked.Read(ref _dropped);
    public long Written => Interlocked.Read(ref _written);
    public long BytesWritten => Interlocked.Read(ref _bytesWritten);

    public string JournalDirectory => _dir;

    public JournalWriter()
    {
        _dir = Environment.GetEnvironmentVariable("JOURNAL_DIR") ?? "/data/events";
        Directory.CreateDirectory(_dir);
    }

    /// <summary>Producer-side fast path. Returns false if the channel is full.</summary>
    public bool TryEnqueue(JobEvent ev)
    {
        if (_channel.Writer.TryWrite(ev))
        {
            Interlocked.Increment(ref _enqueued);
            return true;
        }
        Interlocked.Increment(ref _dropped);
        return false;
    }

    public IReadOnlyList<JournalFileInfo> ListFiles()
    {
        var di = new DirectoryInfo(_dir);
        if (!di.Exists) return Array.Empty<JournalFileInfo>();
        return di.EnumerateFiles("events-*.ndjson.gz")
                 .OrderByDescending(f => f.Name, StringComparer.Ordinal)
                 .Select(f => new JournalFileInfo(f.Name, f.Length, f.LastWriteTimeUtc))
                 .ToList();
    }

    public string ResolveFilePath(string fileName)
    {
        // Reject path traversal.
        if (fileName.Contains("..") || fileName.Contains('/') || fileName.Contains('\\'))
            throw new InvalidOperationException("Invalid journal filename.");
        var path = Path.Combine(_dir, fileName);
        return path;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine($"[Journal] Starting; directory={_dir}, retention={RetentionWindow.TotalDays}d");
        SweepRetention(); // run once at startup

        var lastFlush = DateTime.UtcNow;

        try
        {
            while (await _channel.Reader.WaitToReadAsync(stoppingToken))
            {
                while (_channel.Reader.TryRead(out var ev))
                {
                    try { WriteOne(ev); }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Journal] Write failed: {ex.GetType().Name}: {ex.Message}. Dropping event.");
                    }
                }

                var now = DateTime.UtcNow;
                if (now - lastFlush >= FlushInterval)
                {
                    try { _buf?.Flush(); _gz?.Flush(); _fs?.Flush(); } catch { }
                    lastFlush = now;
                }
                if (now >= _nextSweep)
                {
                    SweepRetention();
                    _nextSweep = now + RetentionSweepInterval;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { /* normal */ }
        finally
        {
            try { _buf?.Flush(); _gz?.Flush(); _fs?.Flush(); } catch { }
            try { _buf?.Dispose(); } catch { }
            try { _gz?.Dispose(); } catch { }
            try { _fs?.Dispose(); } catch { }
            Console.WriteLine($"[Journal] Stopped. enqueued={Enqueued}, written={Written}, dropped={Dropped}, bytes={BytesWritten}");
        }
    }

    private void EnsureCurrentFile()
    {
        var today = DateTime.UtcNow.Date;
        if (_fs != null && _gz != null && _buf != null && today == _currentDayUtc) return;

        // Day rolled over (or first open) — close previous file and open today's.
        try { _buf?.Flush(); } catch { }
        try { _buf?.Dispose(); } catch { }
        try { _gz?.Dispose(); } catch { }
        try { _fs?.Dispose(); } catch { }

        var path = Path.Combine(_dir, $"events-{today:yyyy-MM-dd}.ndjson.gz");
        // Append mode: concatenated gzip members are valid per RFC 1952 and most decoders
        // (gzip, zlib, .NET, Python) handle them transparently.
        _fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read,
            bufferSize: 4096, useAsync: false);
        _gz = new GZipStream(_fs, CompressionLevel.Fastest, leaveOpen: true);
        _buf = new BufferedStream(_gz, 64 * 1024);
        _currentDayUtc = today;
        Console.WriteLine($"[Journal] Open file {path}");
    }

    // Reusable scratch buffer to avoid allocating per event. Single-consumer guarantees
    // safe reuse without locks.
    private readonly MemoryStream _scratch = new(512);
    private static readonly JsonWriterOptions _writerOpts = new() { Indented = false };

    private void WriteOne(JobEvent ev)
    {
        EnsureCurrentFile();

        _scratch.SetLength(0);
        using (var w = new Utf8JsonWriter(_scratch, _writerOpts))
        {
            w.WriteStartObject();
            w.WriteString("job_id", ev.JobId);
            if (!string.IsNullOrEmpty(ev.ParentJobId)) w.WriteString("parent_job_id", ev.ParentJobId);
            w.WriteString("cross_pod", ev.CrossPod);
            w.WriteString("cross_node", ev.CrossNode);
            w.WriteNumber("ts_ns", ev.TsUnixNs);
            w.WriteString("phase", ev.Phase.ToString());
            w.WriteString("mode", ev.Mode.ToString());

            if (!string.IsNullOrEmpty(ev.FileName)) w.WriteString("file_name", ev.FileName);
            if (ev.OriginalSize > 0) w.WriteNumber("original_size", ev.OriginalSize);

            switch (ev.Phase)
            {
                case JobPhase.StageDone:
                    w.WriteString("stage", ev.Stage);
                    w.WriteNumber("stage_ms", ev.StageMs);
                    if (ev.StageAttrChunkCount > 0) w.WriteNumber("chunk_count", ev.StageAttrChunkCount);
                    if (ev.StageAttrBucketCount > 0) w.WriteNumber("bucket_count", ev.StageAttrBucketCount);
                    if (ev.StageAttrBytes > 0) w.WriteNumber("output_bytes", ev.StageAttrBytes);
                    break;
                case JobPhase.BlockDone:
                    w.WriteNumber("block_index", ev.BlockIndex);
                    w.WriteNumber("block_count", ev.BlockCount);
                    w.WriteNumber("block_bytes_in", ev.BlockBytesIn);
                    w.WriteNumber("block_bytes_out", ev.BlockBytesOut);
                    w.WriteNumber("block_refs_found", ev.BlockRefsFound);
                    w.WriteNumber("block_chunks", ev.BlockChunks);
                    w.WriteNumber("block_dc_bytes", ev.BlockDcBytes);
                    w.WriteNumber("block_ms", ev.BlockMs);
                    break;
                case JobPhase.Completed:
                    w.WriteNumber("compressed_size", ev.FinalCompressedSize);
                    w.WriteNumber("refs_found", ev.FinalRefsFound);
                    w.WriteNumber("chunks", ev.FinalChunks);
                    w.WriteNumber("dc_bytes", ev.FinalDcBytes);
                    w.WriteNumber("server_ms", ev.FinalServerMs);
                    w.WriteNumber("wall_ms", ev.FinalWallMs);
                    w.WriteNumber("avg_error_rate", ev.FinalAvgErrorRate);
                    w.WriteNumber("error_payload_bytes", ev.FinalErrorPayloadBytes);
                    if (!string.IsNullOrEmpty(ev.FinalFileId)) w.WriteString("file_id", ev.FinalFileId);
                    break;
                case JobPhase.Failed:
                    if (!string.IsNullOrEmpty(ev.ErrorClass)) w.WriteString("error_class", ev.ErrorClass);
                    if (!string.IsNullOrEmpty(ev.ErrorMessage)) w.WriteString("error_message", ev.ErrorMessage);
                    if (!string.IsNullOrEmpty(ev.ErrorStage)) w.WriteString("error_stage", ev.ErrorStage);
                    break;
            }
            w.WriteEndObject();
        }

        var span = _scratch.GetBuffer().AsSpan(0, (int)_scratch.Length);
        _buf!.Write(span);
        _buf!.WriteByte((byte)'\n');
        Interlocked.Increment(ref _written);
        Interlocked.Add(ref _bytesWritten, span.Length + 1);
    }

    private void SweepRetention()
    {
        try
        {
            var cutoff = DateTime.UtcNow - RetentionWindow;
            var di = new DirectoryInfo(_dir);
            if (!di.Exists) return;
            int removed = 0;
            long bytesFreed = 0;
            foreach (var f in di.EnumerateFiles("events-*.ndjson.gz"))
            {
                if (f.LastWriteTimeUtc < cutoff)
                {
                    bytesFreed += f.Length;
                    try { f.Delete(); removed++; }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Journal] Failed to delete {f.Name}: {ex.Message}");
                    }
                }
            }
            if (removed > 0)
                Console.WriteLine($"[Journal] Retention sweep removed {removed} file(s), freed {bytesFreed:N0} bytes");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Journal] Retention sweep failed: {ex.GetType().Name}: {ex.Message}");
        }
    }

    public sealed record JournalFileInfo(string Name, long SizeBytes, DateTime LastWriteUtc);
}
