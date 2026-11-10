//
// Created by Ayman Geneidy on 07/11/2026.
//

#ifndef LAPTIME_FEED_FEED_HANDLER_H_
#define LAPTIME_FEED_FEED_HANDLER_H_
#include <algorithm>
#include <iterator>
#include <memory>
#include <ostream>
#include <vector>

#include "feed/stream.h"
#include "model/config/feed_handler_config.h"

namespace laptime {
class FeedHandler {
 public:
  explicit FeedHandler(const FeedHandlerConfig& config) {
    std::ranges::transform(
        config.streams, std::back_inserter(streams_),
        [](const auto& c) { return std::make_unique<Stream>(c); });
  }

  void Start() {
    std::ranges::for_each(streams_, &Stream::Start);
  }

  void Stop() {
    std::ranges::for_each(streams_, &Stream::Stop);
  }

  FeedHandler(const FeedHandler&) = delete;
  FeedHandler& operator=(const FeedHandler&) = delete;

  void ReportLatency(std::ostream& out) const {
    out << '\n' << "Per-message latency in µs, steady clock" << '\n';
    for (const auto& stream : streams_) {
      const StreamLatency latency = stream->SummarizeLatency();
      out << '\n';
      PrintLatencyHeader(out, stream->name());
      PrintLatencyRow(out, "  read + TLS (wake → read)", latency.read);
      PrintLatencyRow(out, "  decode (read → t0)", latency.decode);
      PrintLatencyRow(out, "  parse, encode, enqueue (t0 → t1)",
                      latency.enqueue);
      PrintLatencyRow(out, "  total (wake → t1)", latency.total);
    }
    out << '\n'
        << "read + TLS and decode count each socket read once. total also\n"
        << "includes waiting behind earlier messages from the same read.\n";
  }

  ~FeedHandler() { Stop(); }

 private:
  std::vector<std::unique_ptr<Stream>> streams_;
};
}  // namespace laptime

#endif  // LAPTIME_FEED_FEED_HANDLER_H_
