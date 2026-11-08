//
// Created by Ayman Geneidy on 31/10/2026.
//

#ifndef LAPTIME_SOURCES_SOURCE_H_
#define LAPTIME_SOURCES_SOURCE_H_

#include <functional>
#include <string_view>

namespace laptime {

class Source {
 public:
  using MessageHandler = std::function<void(std::string_view)>;
  virtual ~Source() = default;
  virtual void Start() = 0;
  virtual void Stop() = 0;
};

}  // namespace laptime

#endif  // LAPTIME_SOURCES_SOURCE_H_
