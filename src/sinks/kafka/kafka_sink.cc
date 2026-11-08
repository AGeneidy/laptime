//
// Created by Ayman Geneidy on 01/11/2026.
//

#include "sinks/kafka/kafka_sink.h"

#include <iostream>

namespace laptime {

void KafkaSink::Publish(std::string_view key, std::string_view msg_to_publish) {
  RdKafka::ErrorCode err;
  do {
    err = producer_->produce(
        /* Topic name */
        topic_, RdKafka::Topic::PARTITION_UA,
        /* Make a copy of the value */
        RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
        /* Value */
        const_cast<char *>(msg_to_publish.data()), msg_to_publish.size(),
        /* Key */
        const_cast<char *>(key.data()), key.size(), 0, nullptr, nullptr);

    if (err == RdKafka::ERR__QUEUE_FULL) {
      // Queue full: let librdkafka deliver some messages before retrying.
      producer_->poll(1000);
    }
  } while (err == RdKafka::ERR__QUEUE_FULL);

  if (err != RdKafka::ERR_NO_ERROR) {
    std::cerr << "% Failed to produce to topic " << topic_ << ": "
              << RdKafka::err2str(err) << std::endl;
  }

  producer_->poll(0);
}

void KafkaSink::Flush() {
  producer_->flush(10 * 1000 /* wait for max 10 seconds */);
}

KafkaSink::~KafkaSink() {
  Flush();
  dr_cb_->PrintStats();
}

}  // namespace laptime
