//
// Created by Ayman Geneidy on 11/11/2026.
//

#include "sources/file/file_source.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <utility>

namespace laptime {

FileSource::FileSource(const FileConfig& config, MessageHandler on_message)
    : on_message_(std::move(on_message)) {
  std::ifstream in(config.path, std::ios::binary);
  data_.resize(std::filesystem::file_size(config.path));
  if (!in.read(data_.data(), static_cast<std::streamsize>(data_.size()))) {
    throw std::runtime_error("FileSource: can't read " + config.path);
  }

  std::string_view rest = data_;
  while (!rest.empty()) {
    const size_t end = rest.find('\n');
    lines_.push_back(rest.substr(0, end));  // npos: the rest of the file
    if (end == std::string_view::npos) break;
    rest.remove_prefix(end + 1);
  }
}

void FileSource::Start() {
  for (const std::string_view message : lines_) {
    on_message_(message, {});  // no socket, so no wake or read time
  }
}

void FileSource::Stop() {}

}  // namespace laptime
