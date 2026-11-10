//
// Created by Ayman Geneidy on 01/11/2026.
//

#include "sinks/kafka/kafka_sink.h"

#include <charconv>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>

#include "common/clock.h"

namespace laptime {

namespace {
// Adds `nanos` as decimal text, so any Kafka tool can print it.
void AddNanos(RdKafka::Headers& headers, const std::string& key,
              const int64_t nanos) {
  char text[20];
  const char* end = std::to_chars(std::begin(text), std::end(text), nanos).ptr;
  headers.add(key, text, end - text);
}
}  // namespace

void KafkaSink::Publish(std::string_view key, std::string_view msg_to_publish,
                        const MessageTimes& times) {
  std::unique_ptr<RdKafka::Headers> headers(RdKafka::Headers::create());
  AddNanos(*headers, "laptime.wake_ns", times.wake_ns);
  AddNanos(*headers, "laptime.read_ns", times.read_ns);
  AddNanos(*headers, "laptime.start_ns", times.start_ns);
  AddNanos(*headers, "laptime.produce_ns", SteadyNanos());

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
        const_cast<char *>(key.data()), key.size(), 0, headers.get(), nullptr);

    if (err == RdKafka::ERR__QUEUE_FULL) {
      // Queue full: let librdkafka deliver some messages before retrying.
      producer_->poll(1000);
    }
  } while (err == RdKafka::ERR__QUEUE_FULL);

  if (err == RdKafka::ERR_NO_ERROR) {
    headers.release();  // produce() already deleted them: don't delete again
  } else {
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
