using Crossv9.Jobevents;

namespace Controlcenter.Services;

/// <summary>
/// Lock-free-ish ring buffer that holds the last N <see cref="JobEvent"/>s for the
/// live tape view. Capacity is fixed at construction. Old entries are silently
/// overwritten when the buffer wraps.
///
/// Memory budget: capacity × ~256 B. Default 2000 → ~512 KB.
/// Read path takes a snapshot under a single lock; producers write under the same
/// lock but the critical section is a single array index assignment.
/// </summary>
public sealed class JobEventRingBuffer
{
    private readonly JobEvent?[] _buf;
    private readonly int _capacity;
    private long _seq;
    private readonly object _lock = new();

    public JobEventRingBuffer(int capacity = 2000)
    {
        _capacity = capacity;
        _buf = new JobEvent?[capacity];
    }

    public int Capacity => _capacity;

    public void Add(JobEvent ev)
    {
        lock (_lock)
        {
            int idx = (int)(_seq % _capacity);
            _buf[idx] = ev;
            _seq++;
        }
    }

    /// <summary>Snapshot of up to <paramref name="max"/> most recent events, oldest first.</summary>
    public List<JobEvent> Snapshot(int max)
    {
        lock (_lock)
        {
            int count = (int)Math.Min(_seq, _capacity);
            int take = Math.Min(max, count);
            var result = new List<JobEvent>(take);
            // Iterate backwards from newest, then reverse so oldest is first.
            long start = _seq - take;
            for (int i = 0; i < take; i++)
            {
                int idx = (int)((start + i) % _capacity);
                var ev = _buf[idx];
                if (ev != null) result.Add(ev);
            }
            return result;
        }
    }
}
