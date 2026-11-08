//
// Created by Ayman Geneidy on 05/11/2026.
//

#ifndef LAPTIME_ADAPTORS_BINANCE_BINANCE_ADAPTOR_H_
#define LAPTIME_ADAPTORS_BINANCE_BINANCE_ADAPTOR_H_

#include <string_view>
#include <vector>

#include "adaptors/adaptor.h"
#include "model/trade.h"

namespace laptime {

class BinanceAdaptor final : public Adaptor {
  std::vector<Trade> Parse(std::string_view) noexcept override;
};

}  // namespace laptime

#endif  // LAPTIME_ADAPTORS_BINANCE_BINANCE_ADAPTOR_H_
