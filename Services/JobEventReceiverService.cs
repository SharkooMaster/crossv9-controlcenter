using Crossv9.Jobevents;
using Grpc.Core;

namespace Controlcenter.Services;

/// <summary>
/// gRPC server-side handler for <see cref="JobEventService"/>. Accepts a streaming
/// connection per cross pod, fans every event into:
///   1. <see cref="JournalWriter"/> — durable, gzip NDJSON on PVC.
///   2. <see cref="JobEventRingBuffer"/> — last N for the live tape.
///   3. <see cref="LiveBroadcaster"/> — fan-out to SSE subscribers.
///   4. <see cref="JobAggregator"/> — running KPIs (counters, totals).
///
/// All four sinks are non-blocking from this method's perspective, so a slow
/// subscriber or full ring buffer cannot stall ingestion from cross.
/// </summary>
public sealed class JobEventReceiverService : JobEventService.JobEventServiceBase
{
    private readonly JournalWriter _journal;
    private readonly JobEventRingBuffer _ring;
    private readonly LiveBroadcaster _broadcaster;
    private readonly JobAggregator _agg;
    private readonly TopologyTracker _topology;

    public JobEventReceiverService(JournalWriter journal, JobEventRingBuffer ring,
        LiveBroadcaster broadcaster, JobAggregator agg, TopologyTracker topology)
    {
        _journal = journal;
        _ring = ring;
        _broadcaster = broadcaster;
        _agg = agg;
        _topology = topology;
    }

    public override async Task<PushAck> Push(IAsyncStreamReader<JobEvent> requestStream, ServerCallContext context)
    {
        ulong received = 0;
        ulong persisted = 0;
        try
        {
            while (await requestStream.MoveNext(context.CancellationToken))
            {
                var ev = requestStream.Current;
                received++;

                _ring.Add(ev);                          // ~tens of ns under brief lock
                _broadcaster.Publish(ev);               // bounded TryWrite per subscriber
                _agg.Apply(ev);                         // pure in-memory counters
                _topology.Apply(ev);                    // derives client→cross/gateway/agent edges
                if (_journal.TryEnqueue(ev)) persisted++;
            }
        }
        catch (OperationCanceledException) { /* client closed cleanly */ }
        catch (Exception ex)
        {
            Console.WriteLine($"[JobEventReceiver] Stream error: {ex.GetType().Name}: {ex.Message}");
        }

        Console.WriteLine($"[JobEventReceiver] Stream closed; received={received}, persisted={persisted}, peer={context.Peer}");
        return new PushAck { EventsReceived = received, EventsPersisted = persisted };
    }
}
