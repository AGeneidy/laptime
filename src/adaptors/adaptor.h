//
// Created by Ayman Geneidy on 05/11/2026.
//

#ifndef LAPTIME_ADAPTORS_ADAPTOR_H_
#define LAPTIME_ADAPTORS_ADAPTOR_H_

#include <string_view>
#include <vector>

#include "model/trade.h"

namespace laptime {

class Adaptor {
 public:
  virtual ~Adaptor() = default;

  // Returns the trades in `text`: empty if it holds none or is malformed.
  // Never throws.
  virtual std::vector<Trade> Parse(std::string_view text) noexcept = 0;
};

}  // namespace laptime

#endif  // LAPTIME_ADAPTORS_ADAPTOR_H_
