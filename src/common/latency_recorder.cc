//
// Created by Ayman Geneidy on 08/11/2026.
//

#include "common/latency_recorder.h"

#include <algorithm>
#include <iomanip>
#include <ostream>
#include <string>

namespace laptime {

namespace {

std::string WithSeparators(int64_t value) {
  std::string digits = std::to_string(value);
  const size_t first_digit = value < 0 ? 1 : 0;
  for (size_t i = digits.size(); i > first_digit + 3; i -= 3) {
    digits.insert(i - 3, 1, '\'');
  }
  return digits;
}

}  // namespace

LatencySummary LatencyRecorder::GetSummary() const {
  if (current_index_ == 0) return {.dropped = dropped_};
  std::vector sorted(samples_.begin(), samples_.begin() + current_index_);

  std::ranges::sort(sorted);

  const auto at = [&sorted](size_t percent) {
    return sorted[(percent * sorted.size() - 1) / 100];
  };

  return {
      .sample_size = sorted.size(),
      .dropped = dropped_,
      .min = sorted.front(),
      .p50 = at(50),
      .p90 = at(90),
      .p95 = at(95),
      .p99 = at(99),
      .max = sorted.back(),
  };
}

std::ostream& operator<<(std::ostream& out, const LatencySummary& summary) {
  if (summary.sample_size == 0) return out << "  no messages\n";

  out << WithSeparators(static_cast<int64_t>(summary.sample_size))
      << " messages";
  if (summary.dropped > 0) {
    out << " (" << WithSeparators(summary.dropped)
        << " more not recorded: recorder full)";
  }
  out << ":\n";
  const auto row = [&out](const char* label, int64_t nanos) {
    out << "  " << label << ' ' << std::setw(13) << WithSeparators(nanos)
        << " ns\n";
  };
  row("min", summary.min);
  row("p50", summary.p50);
  row("p90", summary.p90);
  row("p95", summary.p95);
  row("p99", summary.p99);
  row("max", summary.max);

  return out;
}
}  // namespace laptime
