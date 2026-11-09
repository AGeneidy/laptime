//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_FEED_STREAM_H_
#define LAPTIME_FEED_STREAM_H_
#include <memory>
#include <string>
#include <string_view>

#include "adaptors/adaptor.h"
#include "adaptors/adaptor_factory.h"
#include "common/latency_recorder.h"
#include "encoders/encoder.h"
#include "encoders/encoder_factory.h"
#include "model/config/stream_config.h"
#include "sinks/sink.h"
#include "sinks/sink_factory.h"
#include "sources/source.h"
#include "sources/source_factory.h"

namespace laptime {

class Stream final {
 public:
  explicit Stream(const StreamConfig& config) : name_(config.name) {
    sink_ = CreateSink(config.sink);
    encoder_ = CreateEncoder(config.encoder);
    adaptor_ = CreateAdaptor(config.adaptor);
    source_ = CreateSource(
        config.source, [this](const std::string_view str) { OnMessage(str); });
  }
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;
  ~Stream();
  void Start();
  void Stop();

  LatencySummary SummarizeLatency() const;
  std::string_view name() const { return name_; }

 private:
  void OnMessage(std::string_view);
  bool stopped_ = false;
  std::string name_;
  Encoder encoder_;
  LatencyRecorder recorder_{};
  std::unique_ptr<Adaptor> adaptor_;
  std::unique_ptr<Sink> sink_;
  std::unique_ptr<Source> source_;
};

}  // namespace laptime

#endif  // LAPTIME_FEED_STREAM_H_
