//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "feed/stream.h"

#include <iostream>
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

StreamLatency Stream::SummarizeLatency() const {
  return {
      .read = read_recorder_.GetSummary(),
      .decode = decode_recorder_.GetSummary(),
      .enqueue = enqueue_recorder_.GetSummary(),
      .total = total_recorder_.GetSummary(),
  };
}

Stream::~Stream() { Stop(); }

void Stream::OnMessage(const std::string_view str, const MessageTimes& times) {
  MessageTimes stamped = times;
  stamped.start_ns = SteadyNanos();
  // Messages from the same socket read share its times, so only the first
  // one's read and decode are its own.
  const bool first_in_read = times.read_ns != last_read_ns_;
  last_read_ns_ = times.read_ns;
  const std::vector<Trade> trades = adaptor_->Parse(str);
  for (const Trade& trade : trades) {
    const std::string_view json = encoder_(trade, buffer_);
    if (json.empty()) {
      std::cerr << "can't encode trade " << trade.trade_id << '\n';
      continue;
    }
    sink_->Publish(trade.symbol, json, stamped);
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

}  // namespace laptime
