<div align="center">

# laptime

**Binance trades in, one normalized `Trade` in the middle, JSON out to Kafka.**

A small C++20 feed handler, built one concept at a time. This README describes the code as it is today.

<br>

<img src="docs/images/architecture.svg" alt="laptime: each Stream passes Binance trades through four swappable stages (Source, Adaptor, Encoder, Sink) to Kafka. FeedHandler owns the Streams." width="100%">

<sub><i>Figure 1. Each Stream reads one feed and passes every trade through four stages, and any stage can be swapped.</i></sub>

</div>

## Reading the diagram

- **A Stream is four stages, in order.** Each message goes through all four:
  1. **`Source`** reads a raw message from the feed.
  2. **`Adaptor`** parses it into a `Trade`.
  3. **`Encoder`** turns the `Trade` into bytes.
  4. **`Sink`** publishes the bytes.

  The arrows carry exactly those three things: message, `Trade`, bytes.
- **Every stage is swappable.** Each stage is an interface. The bold name is what's built today,
  and the grey ones could take its place without touching the other three stages:

  | Stage | Built today | Could also be |
  |---|---|---|
  | `Source` | `WebSocketSource` | a file replay, a UDP multicast feed |
  | `Adaptor` | `BinanceAdaptor` | a Coinbase or Kraken parser |
  | `Encoder` | `ToJson` | Protobuf, SBE |
  | `Sink` | `KafkaSink` | a file, discard (drop everything, for benchmarks) |

  The WebSocket is part of the `Source` stage. It is how today's Source reads from Binance, not
  part of Binance.
- **`FeedHandler` owns the streams.** The stacked cards mean "many". `main.cc` builds one
  `StreamConfig` per feed, and `FeedHandler` creates one `Stream` for each, then starts and stops
  them all.

## Run it

You need a C++20 compiler (Apple clang from the Xcode Command Line Tools), CMake 3.30+,
OpenSSL 3 (`brew install openssl@3`) and Docker for Kafka. The C++ libraries are pinned and
downloaded by CMake on the first configure: [IXWebSocket](https://github.com/machinezone/IXWebSocket)
12.0.1, [librdkafka](https://github.com/confluentinc/librdkafka) 2.16.0 and
[nlohmann/json](https://github.com/nlohmann/json) 3.12.0.

```bash
# 1. Kafka (single node) and the topic
docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic trades

# 2. Build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# 3. Run. Ctrl+C stops it cleanly.
./build/laptime

# 4. In another terminal, watch the trades arrive (key = symbol)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic trades --formatter-property print.key=true
```

The program prints `Connection established` when the WebSocket opens. On exit it prints a
latency table per stream, in microseconds, then `% Kafka delivery stats: <n> delivered, <m> failed`.
From a recorded run:

```
binance-btcusdt                      count      min      p50      p90      p95      p99      max
  read + TLS (wake → read)             251      7.5     92.8    216.4    245.8    492.7   7706.8
  decode (read → t0)                   251      0.6      5.4     12.0     17.0     52.7    439.3
  parse, encode, enqueue (t0 → t1)   7'521      3.0     18.4     21.7     29.3    155.8  19719.2
  total (wake → t1)                  7'521     23.0    575.9   1506.5   1914.9   9724.4  21488.6
```

- **read + TLS** and **decode** happen inside IXWebSocket. A small patch,
  [third_party/ixwebsocket/receive-times.patch](third_party/ixwebsocket/receive-times.patch), which
  CMake applies, times them. One socket read can carry many messages, so these two rows count each
  read once.
- **parse, encode, enqueue** is laptime's own work, until the Kafka produce call returns.
- **total** runs from the moment the socket had data to the end of the enqueue. It includes any
  wait behind earlier messages from the same read.

None of it includes the network, the exchange, or Kafka delivery. Messages that contain no trades
are not counted. Each row keeps its first 10'000 samples, and the table says how many arrived after
that.

Every Kafka record also carries these times as headers (`laptime.wake_ns`, `laptime.read_ns`,
`laptime.start_ns`, `laptime.produce_ns`), so a reader downstream can time it too. To see them:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic trades --formatter-property print.headers=true
```

In **CLion**, open the folder and run the `laptime` target. The red Stop button sends SIGINT, which
triggers the same clean shutdown as Ctrl+C.

## Going deeper

[docs/deep-dive.md](docs/deep-dive.md) explains how it works inside: the four stages, the
`Trade` model, config and factories, ownership, threads, shutdown, what happens when
something goes wrong, how latency is measured, and the timing headers in Kafka.
