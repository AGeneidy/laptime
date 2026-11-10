// Micro-benchmarks for the per-message path, one stage at a time.

#include <benchmark/benchmark.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "adaptors/adaptor.h"
#include "adaptors/adaptor_factory.h"
#include "common/clock.h"
#include "common/latency_recorder.h"
#include "encoders/json/trade_json.h"
#include "model/trade.h"

namespace laptime {
namespace {

// One trade message as Binance sends it with timeUnit=MICROSECOND.
constexpr std::string_view kTradeMessage =
    R"({"e":"trade","E":1794297774767377,"s":"BTCUSDT","t":6834549792,)"
    R"("p":"74884.00000000","q":"0.00018000","T":1794297774767069,)"
    R"("m":true,"M":true})";

void BM_SteadyNanos(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(SteadyNanos());
  }
}
BENCHMARK(BM_SteadyNanos);

void BM_RecordSample(benchmark::State& state) {
  // Each iteration fills a fresh recorder of the default size, so every call
  // takes the write path and memory stays at one recorder however long it runs.
  constexpr size_t kSamples = kDefaultLatencyRecorderSampleSize;
  for (auto _ : state) {
    state.PauseTiming();  // building the recorder isn't what we measure
    LatencyRecorder recorder(kSamples);
    state.ResumeTiming();
    for (size_t i = 0; i < kSamples; ++i) {
      recorder.RecordSample(42);
    }
    benchmark::DoNotOptimize(recorder);
  }
  state.SetItemsProcessed(state.iterations() * kSamples);
}
BENCHMARK(BM_RecordSample);

void BM_Parse(benchmark::State& state) {
  const std::unique_ptr<Adaptor> adaptor =
      CreateAdaptor(BinanceAdaptorConfig{});
  for (auto _ : state) {
    std::vector<Trade> trades = adaptor->Parse(kTradeMessage);
    benchmark::DoNotOptimize(trades);
  }
}
BENCHMARK(BM_Parse);

void BM_ToJson(benchmark::State& state) {
  const std::unique_ptr<Adaptor> adaptor =
      CreateAdaptor(BinanceAdaptorConfig{});
  const Trade trade = adaptor->Parse(kTradeMessage).front();
  for (auto _ : state) {
    std::string json = ToJson(trade);
    benchmark::DoNotOptimize(json);
  }
}
BENCHMARK(BM_ToJson);

void BM_ParseAndEncode(benchmark::State& state) {
  const std::unique_ptr<Adaptor> adaptor =
      CreateAdaptor(BinanceAdaptorConfig{});
  for (auto _ : state) {
    for (const Trade& trade : adaptor->Parse(kTradeMessage)) {
      std::string json = ToJson(trade);
      benchmark::DoNotOptimize(json);
    }
  }
}
BENCHMARK(BM_ParseAndEncode);

}  // namespace
}  // namespace laptime