// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "core/compact_object.h"
#include "core/string_or_view.h"

namespace dfly {
class StringMap;
}  // namespace dfly

namespace dfly::tiering {

// Base class for decoding offloaded values. Used by OpManager do drive correct and efficient
// callback completion for values of different data types
struct Decoder {
  virtual ~Decoder() = default;

  virtual std::unique_ptr<Decoder> Promote() const = 0;  // Type-erase & package oneself
  virtual void Initialize(std::string_view slice) = 0;   // Initialize with fetched value

  virtual size_t EstimateMemoryUsage() const = 0;  // For upload
  virtual void Upload(CompactObj* pv) = 0;         // Peform upload

  bool modified = false;
};

struct StringDecoder : Decoder {
  explicit StringDecoder(const CompactObj& obj);

  std::unique_ptr<Decoder> Promote() const override;
  void Initialize(std::string_view slice) override;
  size_t EstimateMemoryUsage() const override;
  void Upload(CompactObj* pv) override;

  std::string_view Read() const;
  std::string* Write();

 private:
  StringOrView value_;
  std::optional<CompactObj::StrEncoding> encoding_;
};

struct StringMapDecoder : Decoder {
  StringMapDecoder() = default;
  explicit StringMapDecoder(const CompactObj& obj);

  std::unique_ptr<Decoder> Promote() const override;
  void Initialize(std::string_view slice) override;
  size_t EstimateMemoryUsage() const override;
  void Upload(CompactObj* pv) override;

  const StringMap* Read() const;
  StringMap* Write();
  StringMap* Clone() const;

 private:
  std::unique_ptr<StringMap> map_;
};

}  // namespace dfly::tiering
