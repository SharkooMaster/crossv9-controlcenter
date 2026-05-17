# crossv9-controlcenter

Internal command-center / dashboard for the CrossV9 stack. Single-pod, append-only
gzip-NDJSON event journal with 7-day retention. Receives `JobEvent` streams from
`cross` pods over gRPC, fans them into:

- a durable journal on a `ReadWriteOnce` PVC,
- an in-memory ring buffer (last 2 000 events) for the live tape,
- an SSE broadcaster for connected browser tabs,
- a tiny aggregator that maintains running KPIs and the in-flight job set.

Hot-path safety contract: the producer side (`cross`) emits via a single
non-blocking `TryWrite` on a bounded channel. When this service is unreachable,
those writes drop and compression is unaffected.

## Layout

| path                                          | purpose                                  |
|-----------------------------------------------|------------------------------------------|
| `Program.cs`                                  | Kestrel + DI wire-up                     |
| `Protos/JobEvents.proto`                      | gRPC contract (kept in sync with cross)  |
| `Services/JobEventReceiverService.cs`         | gRPC server (`JobEventService.Push`)     |
| `Services/JournalWriter.cs`                   | gzip NDJSON, daily rotation, retention   |
| `Services/RingBuffer.cs`                      | in-memory ring of recent events          |
| `Services/LiveBroadcaster.cs`                 | bounded fan-out to SSE subscribers       |
| `Services/JobAggregator.cs`                   | running KPIs + in-flight set             |
| `Endpoints/Api.cs`                            | REST + SSE + downloads                   |
| `wwwroot/`                                    | dashboard UI                             |
| `Dockerfile`                                  | `mcr.microsoft.com/dotnet/aspnet:8.0`    |

## Ports

- `5000` — gRPC ingest from cross pods (`JobEventService`).
- `5001` — HTTP UI + REST + SSE.

## Memory budget (idle)

- bounded inbound channel: ~2.5 MB
- ring buffer (2 000 events): ~512 KB
- journal stream + buffered gzip: ~80 KB
- aggregator: O(active jobs)
- .NET runtime: ~50–60 MB

`DOTNET_GCHeapHardLimit` is set to 256 MB by Helm; the Kubernetes memory limit
defaults to 512 MiB.

## Storage

The journal lives at `/data/events/events-YYYY-MM-DD.ndjson.gz`. Files older than
7 days are removed by an hourly retention sweep. The dashboard exposes the file
list at `/api/files` and downloads at `/api/files/{name}`.

## REST endpoints

- `GET /api/snapshot` — KPI rollup + in-flight jobs.
- `GET /api/events/recent?max=N` — last N events from the ring buffer.
- `GET /api/events/stream` — text/event-stream live tail.
- `GET /api/files` — list journal files.
- `GET /api/files/{name}` — download a journal file (gzipped NDJSON).
- `GET /health`, `GET /ready` — Kubernetes probes.
