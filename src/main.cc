#include <chrono>
#include <iostream>
#include <thread>

#include "common/stop_signal.h"
#include "feed/feed_handler.h"

namespace laptime {

namespace {

int Run() {
  InstallStopSignalHandlers();

  const auto config = StreamConfig{
      .name = "binance-btcusdt",
      .source = WebSocketConfig{"wss://stream.binance.com:9443/ws/"
                                "btcusdt@trade?timeUnit=MICROSECOND"},
      .sink = KafkaSinkConfig{.brokers = "localhost:9092", .topic = "trades"},
      .adaptor = BinanceAdaptorConfig{},
      .encoder = JsonEncoderConfig{}};

  FeedHandler feed{FeedHandlerConfig{.streams = {config}}};
  feed.Start();

  while (!StopRequested()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  feed.Stop();  // joins the WebSocket thread, so the samples are final
  feed.ReportLatency(std::cout);
  return 0;
}

}  // namespace

}  // namespace laptime

int main() { return laptime::Run(); }
