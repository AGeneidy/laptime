//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "feed/stream.h"

#include <vector>

#include "common/clock.h"

namespace laptime {

void Stream::Start() {
  stopped_ = false;
  source_->Start();
}

void Stream::Stop() {
  if (stopped_) return;  // FeedHandler and ~Stream() both call Stop()
  stopped_ = true;
  source_->Stop();
  sink_->Flush();
}

LatencySummary Stream::SummarizeLatency() const {
  return recorder_.GetSummary();
}

Stream::~Stream() {
  Stop();
}

void Stream::OnMessage(const std::string_view str) {
  const int64_t start = SteadyNanos();
  const std::vector<Trade> trades = adaptor_->Parse(str);
  for (const Trade& trade : trades) {
    sink_->Publish(trade.symbol, encoder_(trade));
  }
  // Messages with no trades (other events, bad JSON) would skew the numbers.
  if (!trades.empty()) recorder_.RecordSample(SteadyNanos() - start);
}

}  // namespace laptime
