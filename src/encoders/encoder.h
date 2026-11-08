//
// Created by Ayman Geneidy on 06/11/2026.
//

#ifndef LAPTIME_ENCODERS_ENCODER_H_
#define LAPTIME_ENCODERS_ENCODER_H_
#include <string>

#include "model/trade.h"

namespace laptime {
using Encoder = std::string (*)(const Trade&);
}
#endif  // LAPTIME_ENCODERS_ENCODER_H_
