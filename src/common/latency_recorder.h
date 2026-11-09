//
// Created by Ayman Geneidy on 08/11/2026.
//

#ifndef LAPTIME_COMMON_LATENCY_RECORDER_H_
#define LAPTIME_COMMON_LATENCY_RECORDER_H_
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <vector>

namespace laptime {

struct LatencySummary {
  size_t sample_size{};
  int64_t dropped{};  // samples that arrived after the recorder was full
  int64_t min{};
  int64_t p50{};
  int64_t p90{};
  int64_t p95{};
  int64_t p99{};
  int64_t max{};
};

inline constexpr size_t kDefaultLatencyRecorderSampleSize = 10'000;
class LatencyRecorder {
 public:
  explicit LatencyRecorder(
      const size_t sample_size = kDefaultLatencyRecorderSampleSize)
      : samples_(sample_size) {}

  // Defined here so the per-message call can be inlined.
  void RecordSample(const int64_t sample) {
    if (current_index_ < samples_.size()) {
      samples_[current_index_++] = sample;
    } else {
      ++dropped_;
    }
  }
  LatencySummary GetSummary() const;

 private:
  std::vector<int64_t> samples_;
  size_t current_index_ = 0;
  int64_t dropped_ = 0;
};
std::ostream& operator<<(std::ostream& out, const LatencySummary& summary);

}  // namespace laptime

#endif  // LAPTIME_COMMON_LATENCY_RECORDER_H_
