# laptime: deep dive

How laptime works inside. For the big picture, start with the [README](../README.md).

1. [A Stream is four stages](#1-a-stream-is-four-stages)
2. [One normalized `Trade`](#2-one-normalized-trade)
3. [Config is plain data](#3-config-is-plain-data)
4. [Factories: `std::visit` picks the class](#4-factories-stdvisit-picks-the-class)
5. [Ownership and lifetime](#5-ownership-and-lifetime)
6. [Threads: who runs what](#6-threads-who-runs-what)
7. [Clean shutdown](#7-clean-shutdown)
8. [What happens when something goes wrong](#8-what-happens-when-something-goes-wrong)
9. [Measuring latency](#9-measuring-latency)
10. [Timing headers in Kafka](#10-timing-headers-in-kafka)

## 1. A Stream is four stages

| Stage | Its job | Contract (interface) | Today | In → out |
|---|---|---|---|---|
| **Source** | Bring raw messages in from a feed (network, file, …) | `Source`: `Start()`, `Stop()`, calls `on_message(text, times)` | `WebSocketSource` | feed → `std::string_view` + `MessageTimes` |
| **Adaptor** | Understand one exchange's format | `Adaptor`: `Parse(text)` → `std::vector<Trade>` | `BinanceAdaptor` | text → `Trade`s |
| **Encoder** | Turn a `Trade` into bytes | `Encoder`: a function pointer, `std::string (*)(const Trade&)` | `ToJson` | `Trade` → `std::string` |
| **Sink** | Deliver bytes somewhere (Kafka, file, …) | `Sink`: `Publish(key, value, times)`, `Flush()` | `KafkaSink` | bytes → destination |

Each stage changes for a different reason. A new exchange needs a new Adaptor. A new wire format
needs a new Encoder. A new destination needs a new Sink. A new transport needs a new Source.
Swapping one stage never touches the other three.

The stages are joined by `Stream::OnMessage`, which is the whole per-message path:

```cpp
void Stream::OnMessage(const std::string_view str, const MessageTimes& times) {
  MessageTimes stamped = times;
  stamped.start_ns = SteadyNanos();
  // Messages from the same socket read share its times, so only the first
  // one's read and decode are its own.
  const bool first_in_read = times.read_ns != last_read_ns_;
  last_read_ns_ = times.read_ns;
  const std::vector<Trade> trades = adaptor_->Parse(str);
  for (const Trade& trade : trades) {
    sink_->Publish(trade.symbol, encoder_(trade), stamped);
  }
  // Messages with no trades (other events, bad JSON) would skew the numbers.
  if (trades.empty()) return;
  const int64_t end = SteadyNanos();
  enqueue_recorder_.RecordSample(end - stamped.start_ns);
  // Sources that can't tell when the bytes arrived leave wake_ns at 0.
  if (times.wake_ns == 0) return;
  total_recorder_.RecordSample(end - times.wake_ns);
  if (first_in_read) {
    read_recorder_.RecordSample(times.read_ns - times.wake_ns);
    decode_recorder_.RecordSample(stamped.start_ns - times.read_ns);
  }
}
```

- `Parse` returns a **vector** because one frame may carry several trades. Binance sends one per
  frame. An empty vector means "nothing to publish" (for example, a frame that isn't JSON).
- The **Encoder is a plain function pointer**, not a class. It has no settings and no state, so
  there is no object to create or own, and calling it is an ordinary function call.
- **Each message carries its times.** The source hands over `MessageTimes`: when the socket had
  data (`wake_ns`) and when the read finished (`read_ns`). `OnMessage` adds `start_ns` (`t0`) to a
  copy and passes it with every trade to the sink, which writes the times into Kafka headers
  ([§10](#10-timing-headers-in-kafka)).
- **Each message is timed.** Four recorders feed the latency report ([§9](#9-measuring-latency)).
  Writing them is why `OnMessage` isn't `const`. The trades are kept in a named vector so the code
  can check afterwards whether there were any.

## 2. One normalized `Trade`

Every exchange is turned into the same struct ([src/model/trade.h](../src/model/trade.h)). Everything
after the Adaptor only sees `Trade` and never knows which exchange it came from.

| Field | Binance key | Example | Notes |
|---|---|---|---|
| `exchange` | (none) | `"binance"` | Set by the adaptor |
| `symbol` | `s` | `"BTCUSDT"` | Also used as the Kafka key |
| `trade_id` | `t` | `6781614918` | Goes up by 1 per trade on each symbol |
| `price` | `p` | `84540.01` | Arrives as text (`"84540.01000000"`), parsed with `strtod` |
| `quantity` | `q` | `9e-05` | Arrives as text, parsed the same way |
| `taker_side` | `m` | `Side::kBuy` | `m` means "the buyer is the maker". `true` → the taker sold → `kSell` |
| `trade_time_us` | `T` | `1792611075432100` | When Binance matched the trade |
| `event_time_us` | `E` | `1792611075432177` | When Binance sent the frame |

Times are in microseconds because the URL asks for `timeUnit=MICROSECOND`.

## 3. Config is plain data

Each stage has one config struct per implementation. The structs are grouped in a
`std::variant`, so a config value holds exactly one of them:

```cpp
struct KafkaSinkConfig {
  std::string brokers;
  std::string topic;
};

using SinkConfig = std::variant<KafkaSinkConfig>;
// Later: std::variant<KafkaSinkConfig, DiscardSinkConfig>
```

`StreamConfig` holds one of each kind (`source`, `adaptor`, `encoder`, `sink`), and
`FeedHandlerConfig` holds a list of `StreamConfig`s. `main.cc` builds them with designated
initializers:

```cpp
const auto config = StreamConfig{
    .name = "binance-btcusdt",
    .source = WebSocketConfig{"wss://stream.binance.com:9443/ws/"
                              "btcusdt@trade?timeUnit=MICROSECOND"},
    .sink = KafkaSinkConfig{.brokers = "localhost:9092", .topic = "trades"},
    .adaptor = BinanceAdaptorConfig{},
    .encoder = JsonEncoderConfig{}};

FeedHandler feed{FeedHandlerConfig{.streams = {config}}};
```

A config only **describes** what to build. It never opens a socket and never creates anything.
All config types live in [src/model/config/](../src/model/config/). An empty struct such as
`BinanceAdaptorConfig{}` still matters: its **type** is what selects the implementation.

## 4. Factories: `std::visit` picks the class

Each kind has a factory header with one `…Of()` overload per config type, and one `Create…()`
function that dispatches between them:

```cpp
inline std::unique_ptr<Sink> SinkOf(const KafkaSinkConfig& config) {
  return std::make_unique<KafkaSink>(config);
}

inline std::unique_ptr<Sink> CreateSink(const SinkConfig& config) {
  return std::visit(
      [&](const auto& local_config) { return SinkOf(local_config); }, config);
}
```

`std::visit` looks inside the variant, finds the type it holds, and calls the `SinkOf` overload
for that type. Overload resolution happens at **compile time**, once for each type the variant can
hold. Add a type to the variant without adding its `…Of()` overload, and the `std::visit` line
**doesn't compile**. You can't get an "unknown type" error at runtime, and there is no `switch`
to keep up to date.

Factories hand back ownership: `CreateSource`, `CreateAdaptor` and `CreateSink` return a
`std::unique_ptr`, and `CreateEncoder` returns a function pointer.

## 5. Ownership and lifetime

- **One owner per object.** `FeedHandler` owns `std::vector<std::unique_ptr<Stream>>`, and each
  `Stream` owns its four stages. There is no `shared_ptr` anywhere.
- **Copying is deleted** on `FeedHandler`, `Stream`, `Sink` and `KafkaSink`. Each of them owns
  something real (a thread, a socket, a Kafka producer). A copy would mean two owners of one socket.
- **Why `unique_ptr<Stream>` and not `Stream` in the vector.** The source's callback captures
  `this` (the `Stream`), so a `Stream` must never move. On the heap its address stays fixed, even
  when the vector grows.
- **Declaration order is destruction order.** Members are destroyed bottom-up, so the source,
  which is declared last, is destroyed first. Its thread is gone before the recorders, adaptor and
  sink it uses are destroyed:

  ```cpp
  Encoder encoder_;
  LatencyRecorder read_recorder_{};
  LatencyRecorder decode_recorder_{};
  LatencyRecorder enqueue_recorder_{};
  LatencyRecorder total_recorder_{};
  int64_t last_read_ns_ = 0;  // read_ns of the previous message
  std::unique_ptr<Adaptor> adaptor_;
  std::unique_ptr<Sink> sink_;
  std::unique_ptr<Source> source_;  // declared last → destroyed first
  ```

- **Nothing runs during construction.** The `Stream` constructor creates the source last, and the
  source only connects when `Start()` is called. No message can arrive before all four stages exist.

## 6. Threads: who runs what

| Thread | Started by | What it runs |
|---|---|---|
| **main** | the OS | Builds everything, sleeps in 100 ms steps, then runs the whole shutdown and prints the latency report |
| **IXWebSocket's thread** (one per stream) | `ix::WebSocket::start()` | Connecting and reconnecting, reading frames, and **our whole per-message path**: `OnMessage` → `Parse` → `ToJson` → `Publish` |
| **librdkafka's threads** | `RdKafka::Producer::create()` | Batching, sending to the broker, retries |

- **No locks in our code.** Each stream's per-message work runs on that stream's own socket
  thread, and streams share nothing. The only cross-thread values are the stop flag (an atomic),
  librdkafka's internal queue (librdkafka does its own locking), and the latency samples (next
  point).
- **Latency samples cross threads once.** The socket thread writes them in `OnMessage`. The main
  thread reads them only after `Stop()` has joined that thread, so the join is all the
  synchronization needed. Reading them while the stream runs would be a data race.
- **`Publish` is an enqueue, not a network send.** `produce()` copies the bytes (`RK_MSG_COPY`) into
  librdkafka's queue and returns. The `std::string` from the encoder can be destroyed right away.
- **Key = symbol.** With `PARTITION_UA`, librdkafka picks the partition by hashing the key, so all
  trades of one symbol land in one partition, in order.
- **Delivery reports.** The broker's yes or no for each message comes back through `dr_cb`, which
  librdkafka calls inside `poll()`. That happens in `Publish` (`poll(0)`, socket thread) and in
  `Flush` (main thread). The two never overlap, because the socket thread has been joined before
  `Flush` runs.

## 7. Clean shutdown

**The signal handler only sets a flag.** A signal can interrupt the program in the middle of
anything, including a `malloc`. Inside a handler, only lock-free atomics are safe. So the
handler stores `true` into a `std::atomic<bool>` (`static_assert`-ed to be lock-free) and
nothing more. The main thread notices within 100 ms and leaves its loop. The handler also
resets the signal to its default, so a **second Ctrl+C kills the process at once**, in case a
shutdown hangs.

**The order is written out in `Run()`.** After the loop, `Run()` stops the feed, and only then
prints the latency report:

```cpp
feed.Stop();  // joins the WebSocket thread, so the samples are final
feed.ReportLatency(std::cout);
```

`FeedHandler::Stop()` calls `Stop()` on every stream:

```cpp
void Stream::Stop() {
  if (stopped_) return;  // FeedHandler and ~Stream() both call Stop()
  stopped_ = true;
  source_->Stop();  // 1. no new messages: close the socket, join its thread
  sink_->Flush();   // 2. then wait (≤ 10 s) for Kafka to confirm the rest
}
```

Stopping the source first means that once the flush starts, nothing new can be published.
`Stop()` is **idempotent**, meaning a second call does nothing, because `Run()`, `~FeedHandler` and
`~Stream` all call it. The report is printed from `Run()`, not from a destructor, so the whole
shutdown reads top to bottom in one place. When `Run()` returns, the members are destroyed (source
first), and `~KafkaSink` prints the delivery stats.

## 8. What happens when something goes wrong

| Situation | What happens |
|---|---|
| A frame that isn't JSON | Parsed with exceptions off (`Json::parse(text, nullptr, false)`): 0 trades, the frame is skipped |
| `p` or `q` isn't a number | `ToDouble` returns `std::nullopt`, and the frame is skipped |
| The connection drops | IXWebSocket reconnects on its own, waiting up to 10 s between tries |
| librdkafka's queue is full | `Publish` waits (`poll(1000)`) and retries. This blocks the socket thread (back-pressure) |
| Kafka rejects a message | Printed when it happens, counted, and totalled at exit |
| A Kafka setting is invalid | The `KafkaSink` constructor throws at startup and the program stops before anything runs |
| IXWebSocket is built without the patch | `websocket_source.cc` stops the build with `#error`, so the times can never be missing silently |

## 9. Measuring latency

![What the latency numbers cover: where each step of a trade runs, which stretches laptime times, and which could be timed or are out of reach](images/latency-coverage.svg)

Every message is timed at four points, all on one clock: `SteadyNanos()`, a wrapper around
`std::chrono::steady_clock`, which never jumps. All four are taken on IXWebSocket's thread.

| Point | When | Taken by |
|---|---|---|
| `wake` | `poll()` saw data on the socket | IXWebSocket, patched (see below) |
| `read` | The read loop, with TLS decryption, finished | IXWebSocket, patched |
| `t0` | `OnMessage` starts | `Stream` |
| `t1` | The last `Publish` returns | `Stream` |

After shutdown, `Run()` prints one table per stream, in microseconds. A recorded run of 7'521
Binance messages:

```
Per-message latency in µs, steady clock

binance-btcusdt                      count      min      p50      p90      p95      p99      max
  read + TLS (wake → read)             251      7.5     92.8    216.4    245.8    492.7   7706.8
  decode (read → t0)                   251      0.6      5.4     12.0     17.0     52.7    439.3
  parse, encode, enqueue (t0 → t1)   7'521      3.0     18.4     21.7     29.3    155.8  19719.2
  total (wake → t1)                  7'521     23.0    575.9   1506.5   1914.9   9724.4  21488.6

read + TLS and decode count each socket read once. total also
includes waiting behind earlier messages from the same read.
% Kafka delivery stats: 7521 delivered, 0 failed
```

| Row | What it covers | Counted |
|---|---|---|
| read + TLS | `wake → read`: IXWebSocket's `SSL_read` loop, the read syscalls and the decryption | Once per socket read |
| decode | `read → t0`: WebSocket frame decoding and the dispatch to `OnMessage` | First message of each read |
| parse, encode, enqueue | `t0 → t1`: `Parse`, `ToJson`, the timing headers and `produce()`. Also `Publish`'s `poll(0)`, which can run delivery reports for earlier messages, and any wait on a full librdkafka queue | Every message with trades |
| total | `wake → t1`: all of the above, plus waiting behind earlier messages from the same read | Every message with trades |

- **Why the counts differ.** One socket read can carry many messages: in the run above, 7'521
  messages arrived in 251 reads, about 30 per read. All messages from one read share its `wake` and
  `read`. Counted per message, one slow read would count 30 times, and after the first message
  `read → t0` is mostly waiting in line. So those two rows take the first message of each read, and
  `total` keeps the waiting, because every message really waits. A read whose first message
  carries no trades isn't counted in those two rows.
- **Where the time goes.** In the run above, laptime's own work is 18 µs at p50, but `total` is
  576 µs. A message in the middle of a 30-message read waits on the same thread for the ones ahead
  of it.
- **Recording never allocates.** Each `LatencyRecorder` allocates its whole vector in the
  constructor (10'000 samples by default). `RecordSample` only writes the next slot, and it is
  defined in the header so it can be inlined into `OnMessage`. Once a recorder is full, further
  samples are counted, and the report says how many weren't recorded.
- **Nearest-rank percentiles.** For p%, `GetSummary()` sorts a copy and takes the sample at index
  `(p * n - 1) / 100`: the smallest sample with at least p% of all samples at or below it. Every
  percentile is a value that was actually measured. With fewer than 100 samples, p99 is the max.
- **The columns line up by display width.** `PrintLatencyRow` pads with `std::format`, which counts
  what you see on screen. `std::setw` counts bytes, and `→` is three bytes in UTF-8.
- **`T` and `E` are context, not a measurement.** Binance stamps `T` when it matches the trade and
  `E` when it sends it. Both are parsed into each `Trade`, but nothing is timed from them.

**Where `wake` and `read` come from.** IXWebSocket doesn't expose them, so laptime patches it.
[third_party/ixwebsocket/receive-times.patch](../third_party/ixwebsocket/receive-times.patch) adds
two `steady_clock` readings around the read in `WebSocketTransport::poll()` and copies them into
every `WebSocketMessage` as `wakeTime` and `readTime`. It also defines
`IXWEBSOCKET_HAS_RECEIVE_TIMES`, which `websocket_source.cc` checks with `#error`. CMake fetches
IXWebSocket at the commit behind v12.0.1, pinned by SHA because a tag can be moved, and applies the
patch:

```cmake
set(IXWEBSOCKET_PATCH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/ixwebsocket/receive-times.patch)
file(SHA256 ${IXWEBSOCKET_PATCH} IXWEBSOCKET_PATCH_SHA256)
FetchContent_Declare(ixwebsocket
        GIT_REPOSITORY https://github.com/machinezone/IXWebSocket.git
        GIT_TAG        64fae7676bd8fe31f7cb4bcde7a6841892dad65e  # v12.0.1
        GIT_SHALLOW    TRUE
        # Reset first, so a re-run never applies the patch on top of itself.
        PATCH_COMMAND  git reset --hard --quiet
        COMMAND  git apply ${IXWEBSOCKET_PATCH}
        # The hash is part of the declaration, so editing the patch re-applies it.
        COMMAND  ${CMAKE_COMMAND} -E echo "IXWebSocket patch ${IXWEBSOCKET_PATCH_SHA256}")
```

- **Reset first.** FetchContent re-runs the patch step in the same folder whenever the declaration
  changes. Without `git reset --hard`, the second `git apply` fails with "patch does not apply".
- **The hash.** FetchContent compares the declaration's text, not the patch file's contents. With
  the patch's SHA-256 in the command, editing the patch makes it re-apply. CMake's output shows
  `IXWebSocket patch <hash>`.
- **To change the patch,** edit the files in `cmake-build-release/_deps/ixwebsocket-src`, save the
  diff, and reload CMake:

  ```bash
  git -C cmake-build-release/_deps/ixwebsocket-src diff > third_party/ixwebsocket/receive-times.patch
  ```

What isn't timed, and what it would take (the bottom rows of the diagram):

| Stretch | Status | Why |
|---|---|---|
| Kafka delivery | Could add | The headers carry `produce_ns` ([§10](#10-timing-headers-in-kafka)), so a reader on the same machine can time produce → receive on the same clock. Inside laptime, `dr_cb` only runs in a later `poll()`, so it would need a poller thread. librdkafka's `Message::latency()` won't do on macOS: there its clock is `gettimeofday` (`src/rdtime.h`), which is wall time. |
| Network | Out of reach | It compares Binance's `E` with our clock, and the two can be milliseconds apart. |
| Kernel wake-up | Out of reach on macOS | macOS only timestamps received datagrams (`SO_TIMESTAMP`), not TCP. Linux has `SO_TIMESTAMPING` for TCP, read with `recvmsg()`, but OpenSSL's socket reads don't use `recvmsg()`, so it would need a custom socket BIO. |

## 10. Timing headers in Kafka

Every record the Kafka sink writes carries its message's times as headers. A reader downstream
can see where the time went, and the trade itself doesn't change.

| Header | Value |
|---|---|
| `laptime.wake_ns` | `wake`: the socket had data |
| `laptime.read_ns` | `read`: the read and TLS decryption finished |
| `laptime.start_ns` | `t0`: `OnMessage` started |
| `laptime.produce_ns` | Just before `produce()` |

One record's headers, as the console consumer prints them:

```
laptime.wake_ns:165344562181791,laptime.read_ns:165344562248916,laptime.start_ns:165344562253375,laptime.produce_ns:165344562326333
```

- **The values are `steady_clock` nanoseconds, as decimal text.** Only differences mean anything:
  the zero point is arbitrary, not a date, so compare them only with readings from the same
  machine. Text was chosen so any Kafka tool can print them.
- **Headers, not the body.** The JSON value is still exactly the `Trade`, so readers that don't
  care never see the times. And `produce_ns` is taken after `ToJson` has already built the body.
- **Who owns the headers.** A successful `produce()` deletes them itself (librdkafka's
  `src-cpp/ProducerImpl.cpp`), and a failed one leaves them alone. So the `unique_ptr` gives them
  up only on success. Deleting them again after a successful `produce()` is a double delete: it
  crashed with SIGSEGV during development.

  ```cpp
  std::unique_ptr<RdKafka::Headers> headers(RdKafka::Headers::create());
  AddNanos(*headers, "laptime.wake_ns", times.wake_ns);
  AddNanos(*headers, "laptime.read_ns", times.read_ns);
  AddNanos(*headers, "laptime.start_ns", times.start_ns);
  AddNanos(*headers, "laptime.produce_ns", SteadyNanos());
  // ... produce(..., headers.get(), nullptr) ...
  if (err == RdKafka::ERR_NO_ERROR) {
    headers.release();  // produce() already deleted them: don't delete again
  } else {
  ```

- **What it costs.** In a standalone benchmark on an Apple silicon Mac, building and freeing the
  four headers took 334–416 ns at p50 and 458–959 ns at p99, over three runs of 200'000. librdkafka
  allocates the header list plus one block per header. On the wire, the names and values add 124
  bytes per record, next to a JSON value of 180–185 bytes. The CPU cost is part of the `t0 → t1`
  row.
- **How to see them:**

  ```bash
  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic trades --max-messages 3 --formatter-property print.headers=true
  ```

  Records written before this change show `NO_HEADERS`.
