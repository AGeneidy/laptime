//
// Created by Ayman Geneidy on 01/11/2026.
//

#ifndef LAPTIME_SINKS_KAFKA_KAFKA_SINK_H_
#define LAPTIME_SINKS_KAFKA_KAFKA_SINK_H_

#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "model/config/sink_config.h"
#include "rdkafkacpp.h"
#include "sinks/sink.h"

namespace laptime {

class KafkaSink final : public Sink {
 public:
  explicit KafkaSink(const KafkaSinkConfig& config) : topic_(config.topic) {
    std::unique_ptr<RdKafka::Conf> conf = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::string errstr;

    if (conf->set("bootstrap.servers", config.brokers, errstr) !=
        RdKafka::Conf::CONF_OK) {
      throw std::runtime_error(errstr);
    }
    dr_cb_ = std::make_unique<ErrorDeliveryReportCb>();
    if (conf->set("dr_cb", dr_cb_.get(), errstr) != RdKafka::Conf::CONF_OK) {
      throw std::runtime_error(errstr);
    }
    producer_ = std::unique_ptr<RdKafka::Producer>(
        RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_) {
      throw std::runtime_error(errstr);
    }
  }

  void Publish(std::string_view, std::string_view, const MessageTimes&) override;

  void Flush() override;

  KafkaSink(const KafkaSink&) = delete;
  KafkaSink(KafkaSink&&) = delete;
  KafkaSink& operator=(const KafkaSink&) = delete;

  ~KafkaSink() override;

 private:
  class ErrorDeliveryReportCb : public RdKafka::DeliveryReportCb {
   public:
    // librdkafka calls this; its name is fixed by RdKafka::DeliveryReportCb.
    void dr_cb(RdKafka::Message& message) override {
      /* If message.err() is non-zero the message delivery failed permanently
       * for the message. */
      if (message.err()) {
        std::cerr << "% Message delivery failed: " << message.errstr()
                  << std::endl;
        failure_count_++;
      } else {
        success_count_++;
      }
    }

    void PrintStats() const {
      std::cout << "% Kafka delivery stats: " << success_count_
                << " delivered, " << failure_count_ << " failed" << std::endl;
    }

   private:
    int64_t success_count_ = 0;
    int64_t failure_count_ = 0;
  };

  std::unique_ptr<ErrorDeliveryReportCb> dr_cb_;
  std::unique_ptr<RdKafka::Producer> producer_;
  std::string topic_;
};

}  // namespace laptime

#endif  // LAPTIME_SINKS_KAFKA_KAFKA_SINK_H_
