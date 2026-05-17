using System.Collections.Concurrent;
using System.Threading.Channels;
using Crossv9.Jobevents;

namespace Controlcenter.Services;

/// <summary>
/// Fans incoming <see cref="JobEvent"/>s out to all currently subscribed SSE clients.
/// Each subscriber owns its own bounded channel: if a browser tab stops reading, only
/// that subscriber drops events; the publisher and all other subscribers are unaffected.
///
/// Memory budget: subscriberCapacity × number of open tabs. Default 500 events × 256 B
/// = ~128 KB per tab.
/// </summary>
public sealed class LiveBroadcaster
{
    private const int SubscriberCapacity = 500;
    private readonly ConcurrentDictionary<Guid, Channel<JobEvent>> _subs = new();

    public int SubscriberCount => _subs.Count;

    public void Publish(JobEvent ev)
    {
        // Snapshot of subscribers — safe because ConcurrentDictionary supports concurrent
        // enumeration; we only read.
        foreach (var kv in _subs)
        {
            // Non-blocking write per subscriber. If full → silently drop for that one.
            kv.Value.Writer.TryWrite(ev);
        }
    }

    public Subscription Subscribe()
    {
        var channel = Channel.CreateBounded<JobEvent>(
            new BoundedChannelOptions(SubscriberCapacity)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false,
            });
        var id = Guid.NewGuid();
        _subs[id] = channel;
        return new Subscription(this, id, channel.Reader);
    }

    private void Unsubscribe(Guid id)
    {
        if (_subs.TryRemove(id, out var ch)) ch.Writer.TryComplete();
    }

    public sealed class Subscription : IDisposable
    {
        private readonly LiveBroadcaster _parent;
        private readonly Guid _id;
        public ChannelReader<JobEvent> Reader { get; }
        internal Subscription(LiveBroadcaster parent, Guid id, ChannelReader<JobEvent> reader)
        {
            _parent = parent;
            _id = id;
            Reader = reader;
        }
        public void Dispose() => _parent.Unsubscribe(_id);
    }
}
