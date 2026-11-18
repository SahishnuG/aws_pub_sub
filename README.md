# Low-Latency Pub/Sub Messaging Server

A multi-threaded publish/subscribe server in C++17 for Linux, built around `epoll`, topic-sharded worker threads, lock-free queues and a custom lock-free memory pool. A Streamlit web client and an epoll-based load generator are included, and the server has been deployed on AWS EC2.

The implementation prioritises per-message latency and throughput over feature completeness. It is a learning and benchmarking project, not a production broker (see [Limitations](#limitations)).

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Protocol](#protocol)
- [Design details](#design-details)
- [Repository layout](#repository-layout)
- [Build and run](#build-and-run)
- [Benchmarks](#benchmarks)
- [Deployment on AWS EC2](#deployment-on-aws-ec2)
- [Limitations](#limitations)
- [Future work](#future-work)

## Features

- 4 edge-triggered `epoll` I/O threads, each with its own `SO_REUSEPORT` listening socket
- 10 worker threads; each topic is owned by exactly one worker, so subscriber sets need no locks
- Moodycamel lock-free MPMC queues between I/O threads and workers
- Custom lock-free memory pool (Treiber stack with ABA-safe tagged indices) for per-command `Task` objects
- Newline-framed text protocol with per-connection read buffers
- `TCP_NODELAY`, non-blocking sockets, `MSG_NOSIGNAL`, raised file-descriptor limit
- Streamlit UI for subscribing, publishing and viewing a live feed
- Two benchmarks: an end-to-end load generator and a baseline-vs-optimized comparison script

## Architecture

```
Browser --HTTP :8501--> Streamlit (app.py)
                           |  PersistentClient (pubsub_client.py), one TCP connection per session
                           v
                  C++ server (server.cpp), TCP :5555
   +---------------------------------------------------------------+
   |  4 I/O threads (epoll, edge-triggered)                        |
   |    accept, read, split lines, parse SUB / PUB                 |
   |    build Task from memory pool, worker = FNV1a(topic) % 10    |
   |                          |  lock-free queue per worker        |
   |                          v                                    |
   |  10 worker threads                                            |
   |    own topic -> {subscriber sockets}, send() to subscribers   |
   +---------------------------------------------------------------+
```

### Message flow

1. **Connect.** The kernel assigns the connection to one of the I/O threads (`SO_REUSEPORT`). The server replies `OK`.
2. **Subscribe.** `SUB <name> <topic>` is parsed by the I/O thread, turned into a `Task`, and enqueued to worker `hash(topic) % 10`. That worker adds the socket to its local subscriber set and replies `Subscribed to <topic>`.
3. **Publish.** `PUB <topic> <message>` follows the same path to the same worker, which sends `Message on <topic>: <message>` to every subscriber and returns the `Task` to the pool.
4. **Disconnect.** The I/O thread sees EOF or an error, enqueues a disconnect task to every worker (it does not know which topics the socket joined), then closes the socket.

## Protocol

Plain text over TCP, one command per line (`\n` terminated).

| Direction | Message | Meaning |
|---|---|---|
| client to server | `SUB <name> <topic>` | Subscribe this connection to `<topic>` |
| client to server | `PUB <topic> <message>` | Publish to `<topic>` |
| server to client | `OK` | Sent once on connect |
| server to client | `Subscribed to <topic>` | Subscription confirmed |
| server to client | `Message on <topic>: <message>` | Delivery to each subscriber |

Limits: topic names are truncated at 63 bytes, payloads at 1023 bytes, and a line must fit in the 4 KB per-connection buffer or the connection is dropped. The `<name>` field is not used as an identity: two connections with the same name are two subscribers. Delivery is fire-and-forget: there is no persistence, no acknowledgement for `PUB`, and messages to a topic with no subscribers are discarded.

## Design details

### Sharding by topic

Every command for a topic is routed to the same worker via an FNV-1a hash, so that worker is the only thread that ever touches the topic's subscriber set. This is a share-nothing design: there are no mutexes on the hot path, and per-topic ordering is preserved because one thread handles the topic in order.

Trade-off: if traffic is concentrated on a few topics, the workers that own them become hot spots while others idle.

### Edge-triggered epoll

Each I/O thread registers sockets with `EPOLLIN | EPOLLET`. epoll reports a socket only when new data arrives, so the code reads until `recv` returns `EAGAIN`. This reduces syscalls but requires fully draining each socket on every event.

### Lock-free queues

Each worker has a Moodycamel `ConcurrentQueue<Task*>`. Multiple I/O threads enqueue and the owning worker dequeues without a mutex.

### Memory pool (`memory_pool.h`)

- `BLOCK_SIZE = 4096`; blocks are allocated once at startup and are 64-byte aligned, so no two blocks share a cache line.
- The free list is a Treiber stack over an index array. The head is a single 64-bit atomic that packs a 32-bit index and a 32-bit tag; the tag increments on every push and pop so a recycled block cannot be mistaken for the one a stalled thread saw (the ABA problem).
- `allocate()` pops with a CAS loop and falls back to `operator new` when the pool is exhausted. `deallocate()` recognises pool pointers by address range and frees fallback pointers normally.
- Memory cost is `pool size x 4 KB`. `MemoryPool taskPool(N)` in `server.cpp` sets `N`: 500,000 blocks is about 2 GB, while 5,000 (about 20 MB) is enough for small demos and fits a 1 GB instance.

Trade-offs: all threads CAS the same head, which becomes a contention point at very high allocation rates, and 4 KB blocks waste memory since a `Task` is about 1.1 KB.

### Worker idle behaviour

Idle workers busy-poll (`try_dequeue` plus `yield()`), which keeps wake-up latency minimal but uses CPU continuously (about 190% on a 2 vCPU instance with no clients). Replacing this with an adaptive spin-then-sleep or blocking wake-up would reduce idle CPU at the cost of some added latency for the first message after a quiet period.

## Repository layout

| File | Purpose |
|---|---|
| `server.cpp` | The pub/sub server |
| `memory_pool.h` | Lock-free memory pool |
| `concurrentqueue.h`, `lightweightsemaphore.h` | Moodycamel queue (third party) |
| `server_baseline.cpp` | Same architecture without the memory pool and lock-free queues (mutex/condition-variable queues, `new`/`delete`), used for comparison |
| `benchmark.cpp` | Epoll load generator: N clients, each subscribes to its own topic and measures round-trip time |
| `final_benchmark.py` | Builds both servers and runs the baseline-vs-optimized comparison |
| `app.py`, `pubsub_client.py` | Streamlit UI and its Python TCP client |
| `DEPLOYMENT.md` | Step-by-step EC2 deployment and operations runbook |

## Build and run

Requirements: Linux (or WSL2), g++ with C++17, Python 3 and `streamlit` for the UI.

```bash
g++ -O2 -std=c++17 -pthread -march=native -o server server.cpp
g++ -O2 -std=c++17 -pthread -march=native -o bench benchmark.cpp
```

Raise the file-descriptor limit before large client counts:

```bash
ulimit -n 100000
```

Start the server:

```bash
./server
```

Quick manual test (any TCP client, for example `nc`):

```bash
nc 127.0.0.1 5555
SUB alice news
PUB news hello
```

Streamlit UI:

```bash
python3 -m venv venv && source venv/bin/activate
pip install streamlit
APP_PASSWORD='choose-a-strong-password' streamlit run app.py
```

Load generator:

```bash
./bench 1000        # number of simulated clients
```

## Benchmarks

### 1. End-to-end load test

`benchmark.cpp` opens N connections. Each subscribes to its own topic, publishes a message carrying a nanosecond timestamp, and measures round-trip time when the message is delivered back. Throughput, average, p50 and p99 are printed every second.

Environment: Intel i7-12650H, Ubuntu on WSL2, g++ `-O2 -march=native`, client and server on the same machine over loopback.

| Clients | Throughput (msg/s) | Avg latency (us) | p50 (us) | p99 (us) |
|--------:|-------------------:|-----------------:|---------:|---------:|
| 10      | 69,422             | 143.9            | 124.8    | 464.9    |
| 500     | 154,825            | 3,229.4          | 2,972.5  | 6,619.8  |
| 1000    | 168,339            | 5,939.2          | 5,691.9  | 10,853.5 |
| 2000    | 124,450            | 16,141.6         | 15,968.5 | 24,403.6 |

Throughput peaks at about 1000 clients and drops at 2000, where scheduling overhead and per-socket buffer limits dominate. The long p99 tail at high concurrency points to scheduling delays or slow subscriber drains. WSL2 adds virtualization overhead, so use these numbers for relative comparison rather than absolute claims.

### 2. Baseline vs optimized

`server_baseline.cpp` keeps the same epoll I/O, sharding, parsing and fan-out as `server.cpp`, and removes only the two optimisations: tasks use `new`/`delete` instead of the memory pool, and workers use a mutex-protected `std::deque` with a condition variable instead of the lock-free queue.

```bash
python3 final_benchmark.py --messages 10000 --producers 4 --consumers 4 --warmup 1000 --runs 5
```

The script compiles both servers, runs them alternately (so machine drift affects both equally), and reports elapsed time, throughput, median latency and p99.9 latency.

Result on an AWS EC2 Ubuntu instance (10,000 messages, 4 publisher and 4 subscriber connections, 5 runs):

| | Throughput (msg/s) | Median (us) | p99.9 (us) |
|---|---:|---:|---:|
| Baseline | 5,655 | 130 | 3,069 |
| Optimized | 10,548 | 93 | 473 |
| Change | 1.87x | 28% lower | 84.6% lower |

How to read this result:

- The optimized build in this test busy-polls, so part of its advantage comes from workers that are already awake rather than from the queue and pool alone.
- The script sends one message at a time and waits for all subscribers to receive it, on a single topic (so a single worker). It measures delivery latency, not scaling under contention.
- Real-time measurement on a shared VM is noisy; compare ratios, not absolute values.

## Deployment on AWS EC2

The server and Streamlit UI run on an Ubuntu EC2 instance. Only SSH (restricted to one IP) and the Streamlit port are open in the security group, and the server port is never exposed: Streamlit connects to it over `127.0.0.1`. The server has no authentication of its own, so access control relies on this plus the Streamlit password gate.

See [DEPLOYMENT.md](DEPLOYMENT.md) for the full procedure: security groups, SSH, building, `tmux`/`nohup` process management, restarting, and troubleshooting.

## Limitations

- **No delivery guarantees.** Messages are not persisted or acknowledged, and subscribers that cannot keep up are dropped.
- **No backpressure.** Slow subscribers are pruned when `send()` returns `EAGAIN`. The pruning currently calls `close()` from a worker thread while the I/O thread still tracks the descriptor, so a reused descriptor number could receive stale deliveries.
- **Partial writes are not handled.** If `send()` writes fewer bytes than requested, the rest is dropped.
- **Parsing is minimal.** Lines shorter than the command prefix are not bounds-checked, and oversized lines disconnect the client.
- **Busy-polling workers** consume CPU even when idle (see [Worker idle behaviour](#worker-idle-behaviour)).
- **Topic-based sharding** can cause load imbalance with skewed traffic.
- **Fixed 4 KB pool blocks** waste memory for the roughly 1.1 KB `Task`.
- **No authentication or TLS** on the server port.
- **Single machine only:** no clustering, replication or cross-datacenter support.
- **Python UI client** reads with a single `recv(1024)` and does not frame messages, so bursts can merge into one displayed entry. Closed browser tabs leave their server connections open.

## Future work

Correctness and robustness:
- Let only the I/O thread close sockets, with workers requesting disconnects.
- Handle partial `send()` with per-subscriber output buffers and `EPOLLOUT`.
- Idle timeouts and heartbeats to reclaim leaked connections.

Performance:
- Spin-then-sleep or `eventfd` wake-ups to cut idle CPU.
- Right-size pool blocks to the `Task` size, with a `static_assert` on `sizeof(Task)`.
- Multi-topic, open-loop benchmarks to measure scaling under contention.
- Batching multiple messages per syscall, and zero-copy payload sharing across subscribers.
- NUMA-aware per-worker pools and dynamic shard rebalancing for skewed topics.

Features:
- Persistent log and replay, per-subscriber queues with flow control, and optional authentication.

## Dependencies

- [`concurrentqueue.h`](https://github.com/cameron314/concurrentqueue) (Moodycamel lock-free MPMC queue) and its `lightweightsemaphore.h` companion.
- Python 3 and Streamlit, only for the web UI.
