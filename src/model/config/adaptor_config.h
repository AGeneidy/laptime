//
// Created by Ayman Geneidy on 05/11/2026.
//

#ifndef LAPTIME_MODEL_CONFIG_ADAPTOR_CONFIG_H_
#define LAPTIME_MODEL_CONFIG_ADAPTOR_CONFIG_H_

#include <variant>

namespace laptime {

struct BinanceAdaptorConfig {};

using AdaptorConfig = std::variant<BinanceAdaptorConfig>;

}  // namespace laptime

#endif  // LAPTIME_MODEL_CONFIG_ADAPTOR_CONFIG_H_
