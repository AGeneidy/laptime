//
// Created by Ayman Geneidy on 11/11/2026.
//

#ifndef LAPTIME_SOURCES_FILE_FILE_SOURCE_H_
#define LAPTIME_SOURCES_FILE_FILE_SOURCE_H_

#include <string>
#include <string_view>
#include <vector>

#include "model/config/source_config.h"
#include "sources/source.h"

namespace laptime {

class FileSource final : public Source {
 public:
  explicit FileSource(const FileConfig& config, MessageHandler on_message);
  // lines_ points into data_, so a copy would point into the original.
  FileSource(const FileSource&) = delete;
  FileSource& operator=(const FileSource&) = delete;

  void Start() override;

  void Stop() override;

 private:
  std::string data_;
  std::vector<std::string_view> lines_;
  MessageHandler on_message_;
};

}  // namespace laptime

#endif  // LAPTIME_SOURCES_FILE_FILE_SOURCE_H_
