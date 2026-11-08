//
// Created by Ayman Geneidy on 06/11/2026.
//

#include "feed/stream.h"

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

Stream::~Stream() { Stop(); }

void Stream::OnMessage(const std::string_view str) const {
  for (const Trade& trade : adaptor_->Parse(str)) {
    sink_->Publish(trade.symbol, encoder_(trade));
  }
}

}  // namespace laptime
