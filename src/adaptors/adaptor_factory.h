//
// Created by Ayman Geneidy on 05/11/2026.
//

#ifndef LAPTIME_ADAPTORS_ADAPTOR_FACTORY_H_
#define LAPTIME_ADAPTORS_ADAPTOR_FACTORY_H_

#include <memory>
#include <variant>

#include "adaptors/adaptor.h"
#include "adaptors/binance/binance_adaptor.h"
#include "model/config/adaptor_config.h"

namespace laptime {

inline std::unique_ptr<Adaptor> AdaptorOf(const BinanceAdaptorConfig&) {
  return std::make_unique<BinanceAdaptor>();
}

inline std::unique_ptr<Adaptor> CreateAdaptor(const AdaptorConfig& config) {
  return std::visit([](const auto& c) { return AdaptorOf(c); }, config);
}

}  // namespace laptime

#endif  // LAPTIME_ADAPTORS_ADAPTOR_FACTORY_H_
