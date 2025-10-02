// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/decoders.h"

#include "core/string_map.h"
#include "server/table.h"

namespace dfly::tiering {

namespace {

std::unique_ptr<StringMap> DecodeStringMap(absl::Span<const char> data) {
  using namespace std;
  auto map = make_unique<StringMap>();

  const char* ptr = data.begin();
  uint32_t entries = absl::little_endian::Load32(ptr);
  ptr += 4;

  for (uint32_t i = 0; i < entries; i++) {
    uint32_t len = absl::little_endian::Load32(ptr);
    ptr += 4;

    string key;
    key.resize(len);
    memcpy(key.data(), ptr, len);
    ptr += len;

    len = absl::little_endian::Load32(ptr);
    ptr += 4;

    string value;
    value.resize(len);
    memcpy(value.data(), ptr, len);
    ptr += len;

    map->AddOrUpdate(key, value);
  }

  return map;
}
}  // namespace

StringDecoder::StringDecoder(const CompactObj& obj) : encoding_{obj.GetStrEncoding()} {
}

std::unique_ptr<Decoder> StringDecoder::Promote() const {
  return std::make_unique<StringDecoder>(*this);
}

void StringDecoder::Initialize(std::string_view slice) {
  value_ = encoding_->Decode(slice);
  encoding_.reset();  // no longer needed
}

size_t StringDecoder::EstimateMemoryUsage() const {
  return value_.view().length();
}

void StringDecoder::Upload(CompactObj* obj) {
  obj->Materialize(Read(), !encoding_.has_value());
}

std::string_view StringDecoder::Read() const {
  return value_.view();
}

std::string* StringDecoder::Write() {
  modified = true;
  return value_.BorrowMut();
}

StringMapDecoder::StringMapDecoder(const CompactObj& obj) {
}

std::unique_ptr<Decoder> StringMapDecoder::Promote() const {
  DCHECK(!map_);
  return std::make_unique<StringMapDecoder>();  // no data stored
}

void StringMapDecoder::Initialize(std::string_view slice) {
  map_ = DecodeStringMap(slice);
}

size_t StringMapDecoder::EstimateMemoryUsage() const {
  return map_->ObjMallocUsed();
}

void StringMapDecoder::Upload(CompactObj* obj) {
  map_.reset();
}

const StringMap* StringMapDecoder::Read() const {
  return &*map_;
}

StringMap* StringMapDecoder::Write() {
  modified = true;
  return &*map_;
}

StringMap* StringMapDecoder::Clone() const {
  StringMap* clone = new StringMap{};
  for (const auto p : *map_)
    clone->AddOrUpdate(p.first, p.second);
  return clone;
}

}  // namespace dfly::tiering
