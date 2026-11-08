//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_FEED_STREAM_H_
#define LAPTIME_FEED_STREAM_H_
#include <memory>

#include "adaptors/adaptor.h"
#include "adaptors/adaptor_factory.h"
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
  explicit Stream(const StreamConfig& config) {
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

 private:
  void OnMessage(std::string_view) const;
  bool stopped_ = false;
  Encoder encoder_;
  std::unique_ptr<Adaptor> adaptor_;
  std::unique_ptr<Sink> sink_;
  std::unique_ptr<Source> source_;
};

}  // namespace laptime

#endif  // LAPTIME_FEED_STREAM_H_
