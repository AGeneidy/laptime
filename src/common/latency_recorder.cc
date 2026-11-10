//
// Created by Ayman Geneidy on 08/11/2026.
//

#include "common/latency_recorder.h"

#include <algorithm>
#include <format>
#include <ostream>
#include <string>

namespace laptime {

namespace {

// Column widths of the report table.
constexpr int kLabelWidth = 34;
constexpr int kCountWidth = 8;
constexpr int kValueWidth = 9;

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

void PrintLatencyHeader(std::ostream& out, const std::string_view title) {
  // std::format pads by what you see, so "→" and "µ" take one column each.
  out << std::format("{:<{}}{:>{}}", title, kLabelWidth, "count", kCountWidth);
  for (const char* column : {"min", "p50", "p90", "p95", "p99", "max"}) {
    out << std::format("{:>{}}", column, kValueWidth);
  }
  out << '\n';
}

void PrintLatencyRow(std::ostream& out, const std::string_view label,
                     const LatencySummary& summary) {
  out << std::format("{:<{}}{:>{}}", label, kLabelWidth,
                     WithSeparators(static_cast<int64_t>(summary.sample_size)),
                     kCountWidth);
  const auto cell = [&out](const int64_t nanos) {
    out << std::format("{:>{}.1f}", static_cast<double>(nanos) / 1'000,
                       kValueWidth);
  };
  if (summary.sample_size > 0) {
    cell(summary.min);
    cell(summary.p50);
    cell(summary.p90);
    cell(summary.p95);
    cell(summary.p99);
    cell(summary.max);
  }
  out << '\n';
  if (summary.dropped > 0) {
    out << std::format("{:<{}}({} more not recorded: recorder full)\n", "",
                       kLabelWidth, WithSeparators(summary.dropped));
  }
}
}  // namespace laptime
