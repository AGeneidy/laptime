// Micro-benchmarks for the per-message path, one stage at a time.

#include <benchmark/benchmark.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <vector>

#include "adaptors/adaptor.h"
#include "adaptors/adaptor_factory.h"
#include "common/clock.h"
#include "common/latency_recorder.h"
#include "encoders/json/trade_json.h"
#include "feed/stream.h"
#include "model/trade.h"

// Every heap allocation in this program goes through this operator new, so
// a benchmark can count how many its code makes.
namespace {
std::atomic<int64_t> allocation_count{0};
}  // namespace

void* operator new(std::size_t size) {
  allocation_count.fetch_add(1, std::memory_order_relaxed);
  // malloc(0) may return nullptr, which would look like running out.
  if (void* p = std::malloc(size == 0 ? 1 : size)) return p;
  throw std::bad_alloc();
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

namespace laptime {
namespace {

int64_t AllocationCount() {
  return allocation_count.load(std::memory_order_relaxed);
}

// For example "25.0 allocations per trade".
std::string AllocationsPer(int64_t allocations, int64_t items,
                           std::string_view item) {
  return std::format("{:.1f} allocations per {}",
                     static_cast<double>(allocations) / items, item);
}

// One trade message as Binance sends it with timeUnit=MICROSECOND.
constexpr std::string_view kTradeMessage =
    R"({"e":"trade","E":1794297774767377,"s":"BTCUSDT","t":6834549792,)"
    R"("p":"74884.00000000","q":"0.00018000","T":1794297774767069,)"
    R"("m":true,"M":true})";

void BM_SteadyNanos(benchmark::State& state) {
  const int64_t allocations_before = AllocationCount();
  for (auto _ : state) {
    benchmark::DoNotOptimize(SteadyNanos());
  }
  const int64_t allocations = AllocationCount() - allocations_before;
  state.SetLabel(AllocationsPer(allocations, state.iterations(), "reading"));
}
BENCHMARK(BM_SteadyNanos);

void BM_RecordSample(benchmark::State& state) {
  // Each iteration fills a fresh recorder of the default size, so every call
  // takes the write path and memory stays at one recorder however long it runs.
  constexpr size_t kSamples = kDefaultLatencyRecorderSampleSize;
  int64_t allocations = 0;
  for (auto _ : state) {
    state.PauseTiming();  // building the recorder isn't what we measure
    LatencyRecorder recorder(kSamples);
    state.ResumeTiming();
    // Count only the RecordSample calls, not building the recorder.
    const int64_t allocations_before = AllocationCount();
    for (size_t i = 0; i < kSamples; ++i) {
      recorder.RecordSample(42);
    }
    allocations += AllocationCount() - allocations_before;
    benchmark::DoNotOptimize(recorder);
  }
  state.SetItemsProcessed(state.iterations() * kSamples);
  state.SetLabel(
      AllocationsPer(allocations, state.iterations() * kSamples, "sample"));
}
BENCHMARK(BM_RecordSample);

void BM_Parse(benchmark::State& state) {
  const std::unique_ptr<Adaptor> adaptor =
      CreateAdaptor(BinanceAdaptorConfig{});
  const int64_t allocations_before = AllocationCount();
  for (auto _ : state) {
    std::vector<Trade> trades = adaptor->Parse(kTradeMessage);
    benchmark::DoNotOptimize(trades);
  }
  const int64_t allocations = AllocationCount() - allocations_before;
  state.SetLabel(AllocationsPer(allocations, state.iterations(), "trade"));
}
BENCHMARK(BM_Parse);

void BM_ToJson(benchmark::State& state) {
  const std::unique_ptr<Adaptor> adaptor =
      CreateAdaptor(BinanceAdaptorConfig{});
  const Trade trade = adaptor->Parse(kTradeMessage).front();
  const int64_t allocations_before = AllocationCount();
  std::array<char, 1024> buffer;
  for (auto _ : state) {
    std::string_view json = ToJson(trade, buffer);
    benchmark::DoNotOptimize(json);
  }
  const int64_t allocations = AllocationCount() - allocations_before;
  state.SetLabel(AllocationsPer(allocations, state.iterations(), "trade"));
}
BENCHMARK(BM_ToJson);

void BM_ParseAndEncode(benchmark::State& state) {
  const std::unique_ptr<Adaptor> adaptor =
      CreateAdaptor(BinanceAdaptorConfig{});
  const int64_t allocations_before = AllocationCount();
  std::array<char, 1024> buffer;
  for (auto _ : state) {
    for (const Trade& trade : adaptor->Parse(kTradeMessage)) {
      std::string_view json = ToJson(trade, buffer);
      benchmark::DoNotOptimize(json);
    }
  }
  const int64_t allocations = AllocationCount() - allocations_before;
  state.SetLabel(AllocationsPer(allocations, state.iterations(), "trade"));
}
BENCHMARK(BM_ParseAndEncode);

void BM_ReplayStream(benchmark::State& state) {
  // The whole path for a recorded feed: FileSource → Stream::OnMessage
  // (parse, encode, timing, recorders) → DiscardSink.
  Stream stream(StreamConfig{
      .name = "replay",
      .source =
          FileConfig{LAPTIME_TESTDATA_DIR "/binance-btcusdt-trades.jsonl"},
      .sink = DiscardSinkConfig{},
      .adaptor = BinanceAdaptorConfig{},
      .encoder = JsonEncoderConfig{}});
  const int64_t allocations_before = AllocationCount();
  const int64_t begin = SteadyNanos();
  for (auto _ : state) {
    stream.Start();  // replays every line on this thread
  }
  const int64_t elapsed = SteadyNanos() - begin;
  // Before SummarizeLatency(), which allocates to sort the samples.
  const int64_t allocations = AllocationCount() - allocations_before;
  const LatencySummary summary = stream.SummarizeLatency().enqueue;
  const int64_t trades =
      static_cast<int64_t>(summary.sample_size) + summary.dropped;
  state.SetLabel(std::format("{} trades per replay, {:.2f} µs per trade and {}",
                             trades / state.iterations(),
                             elapsed / 1000.0 / trades,
                             AllocationsPer(allocations, trades, "trade")));
}
BENCHMARK(BM_ReplayStream)->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace laptime
