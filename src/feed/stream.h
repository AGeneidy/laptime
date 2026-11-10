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

// One summary per stretch of the receive path, all on the steady clock.
struct StreamLatency {
  LatencySummary read;     // wake → read: the read and TLS decryption
  LatencySummary decode;   // read → t0: WebSocket decode
  LatencySummary enqueue;  // t0 → t1: parse, encode, Kafka enqueue
  LatencySummary total;    // wake → t1: all of it, plus any wait behind
                           // earlier messages from the same read
};

class Stream final {
 public:
  explicit Stream(const StreamConfig& config) : name_(config.name) {
    sink_ = CreateSink(config.sink);
    encoder_ = CreateEncoder(config.encoder);
    adaptor_ = CreateAdaptor(config.adaptor);
    source_ = CreateSource(config.source, [this](const std::string_view str,
                                                 const MessageTimes& times) {
      OnMessage(str, times);
    });
  }
  Stream(const Stream&) = delete;
  Stream& operator=(const Stream&) = delete;
  ~Stream();
  void Start();
  void Stop();

  StreamLatency SummarizeLatency() const;
  std::string_view name() const { return name_; }

 private:
  void OnMessage(std::string_view, const MessageTimes&);
  bool stopped_ = false;
  std::string name_;
  Encoder encoder_;
  LatencyRecorder read_recorder_{};
  LatencyRecorder decode_recorder_{};
  LatencyRecorder enqueue_recorder_{};
  LatencyRecorder total_recorder_{};
  int64_t last_read_ns_ = 0;  // read_ns of the previous message
  std::unique_ptr<Adaptor> adaptor_;
  std::unique_ptr<Sink> sink_;
  std::unique_ptr<Source> source_;
};

}  // namespace laptime

#endif  // LAPTIME_FEED_STREAM_H_
