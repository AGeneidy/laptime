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
    for (const auto& stream : streams_) {
      out << '\n' << "latency for stream " << stream->name() << ":\n"
          << stream->SummarizeLatency();
    }
  }

  ~FeedHandler() { Stop(); }

 private:
  std::vector<std::unique_ptr<Stream>> streams_;
};
}  // namespace laptime

#endif  // LAPTIME_FEED_FEED_HANDLER_H_
