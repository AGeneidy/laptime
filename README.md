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
latency table per stream (min, p50, p90, p95, p99, max in nanoseconds), then
`% Kafka delivery stats: <n> delivered, <m> failed`.

Each latency sample is the time one WebSocket message spends inside laptime: from the moment
IXWebSocket hands it over, through parsing, encoding and the Kafka produce call, until the call
returns. It does not include the network, the exchange, the WebSocket layer (TLS decryption and
frame decoding), or Kafka delivery. Messages that contain no trades are not counted. The first
10'000 samples are kept, and the table says how many arrived after that.

In **CLion**, open the folder and run the `laptime` target. The red Stop button sends SIGINT, which
triggers the same clean shutdown as Ctrl+C.

## Going deeper

[docs/deep-dive.md](docs/deep-dive.md) explains how it works inside: the four stages, the
`Trade` model, config and factories, ownership, threads, shutdown, and what happens when
something goes wrong.
