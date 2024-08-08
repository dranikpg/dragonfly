// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string_view>

#include "facade/facade_types.h"
#include "facade/op_status.h"
#include "io/io.h"

namespace facade {

// Reply mode allows filtering replies.
enum class ReplyMode {
  NONE,      // No replies are recorded
  ONLY_ERR,  // Only errors are recorded
  FULL       // All replies are recorded
};

// Base class for all reply builders. Offer a simple high level interface for controlling output
// modes and sending basic response types.
class SinkReplyBuilder {
  struct GuardBase {
    bool prev;
    SinkReplyBuilder* rb;
  };

 public:
  constexpr static size_t kMaxInlineSize = 32;
  constexpr static size_t kMaxBufferSize = 8192;

  explicit SinkReplyBuilder(io::Sink* sink) : sink_(sink) {
  }

  // Use with care: All send calls within a scope must keep their data alive!
  // This allows to fully eliminate copies for batches of data by using vectorized io.
  struct ReplyScope : GuardBase {
    explicit ReplyScope(SinkReplyBuilder* rb) : GuardBase{std::exchange(rb->scoped_, true), rb} {
    }

    ~ReplyScope();
  };

  // Reduce number of send calls by aggregating responses in a buffer. Prefer ReplyScope
  // if it's conditions are met.
  struct ReplyAggregator : GuardBase {
    explicit ReplyAggregator(SinkReplyBuilder* rb)
        : GuardBase{std::exchange(rb->batched_, true), rb} {
    }

    ~ReplyAggregator();
  };

  std::error_code GetError() const {
    return ec_;
  }

  size_t UsedMemory() const {
    return buffer_.Capacity();
  }

  bool IsSendActive() {
    return false;
  }

  void SetBatchMode(bool b) {
    batched_ = b;
  }

  void FlushBatch() {
    Flush();
  }

  void CloseConnection() {
  }

  void ExpectReply() {
  }

 public:  // High level interface
  virtual void SendLong(long val) = 0;
  virtual void SendSimpleString(std::string_view str) = 0;

  virtual void SendStored() = 0;
  virtual void SendSetSkipped() = 0;
  void SendOk() {
    SendSimpleString("OK");
  }

  virtual void SendError(std::string_view str, std::string_view type = {}) = 0;  // MC and Redis
  void SendError(OpStatus status);
  void SendError(ErrorReply error);
  virtual void SendProtocolError(std::string_view str) = 0;

 protected:
  void WriteI(std::string_view str) {
    str.size() > kMaxInlineSize ? WriteRef(str) : WritePiece(str);
  }

  template <size_t S> void WriteI(const char (&arr)[S]) {
    WritePiece(std::string_view{arr, S - 1});
  }

  template <typename... Args> void Write(Args&&... strs) {
    (WriteI(strs), ...);
  }

  void Flush();        // Send all accumulated data and reset to clear state
  void FinishScope();  // Called when scope ends

  char* ReservePiece(size_t size);        // Reserve size bytes from buffer
  void CommitPiece(size_t size);          // Mark size bytes from buffer as used
  void WritePiece(std::string_view str);  // Reserve + memcpy + Commit

  void WriteRef(std::string_view str);  // Add iovec bypassing buffer
  void NextVec(std::string_view str);

 private:
  io::Sink* sink_;
  std::error_code ec_;

  bool scoped_ = false, batched_ = false;

  size_t total_size_ = 0;  // sum of vec_ lengths
  base::IoBuf buffer_;
  std::vector<iovec> vecs_;
  std::vector<unsigned> ext_indices_;
};

class MCReplyBuilder : public SinkReplyBuilder {
  bool noreply_;

 public:
  explicit MCReplyBuilder(::io::Sink* stream);

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
  void SendNotFound();

  void SendValue(std::string_view key, std::string_view value, uint64_t mc_ver, uint32_t mc_flag);
  void SendSimpleString(std::string_view str) final;
  void SendProtocolError(std::string_view str) final;

  void SetNoreply(bool noreply) {
    noreply_ = noreply;
  }

  bool NoReply() const;
};

// Redis reply builder interface for sending RESP data.
class RedisReplyBuilder2Base : public SinkReplyBuilder {
 public:
  enum CollectionType { ARRAY, SET, MAP, PUSH };

  enum VerbatimFormat { TXT, MARKDOWN };

  explicit RedisReplyBuilder2Base(io::Sink* sink) : SinkReplyBuilder(sink) {
  }

  virtual void SendNull();
  void SendSimpleString(std::string_view str) override;
  virtual void SendBulkString(std::string_view str);  // RESP: Blob String

  void SendLong(long val) override;
  virtual void SendDouble(double val);  // RESP: Number

  virtual void SendNullArray();
  virtual void StartCollection(unsigned len, CollectionType ct);

  using SinkReplyBuilder::SendError;
  void SendError(std::string_view str, std::string_view type = {}) override;
  void SendProtocolError(std::string_view str) override;

  bool IsResp3() const {
    return resp3_;
  }

  void SetResp3(bool resp3) {
    resp3_ = resp3;
  }

 private:
  void WriteIntWithPrefix(char prefix, int64_t val);  // FastIntToBuffer directly into ReservePiece

  bool resp3_ = false;
};

// Non essential redis reply builder functions
class RedisReplyBuilder : public RedisReplyBuilder2Base {
 public:
  RedisReplyBuilder(io::Sink* sink) : RedisReplyBuilder2Base(sink) {
  }

  void SendSimpleStrArr(const facade::ArgRange& strs);
  void SendBulkStrArr(const facade::ArgRange& strs, CollectionType type = ARRAY);
  void SendScoredArray(absl::Span<const std::pair<std::string, double>> arr, bool with_scores);

  void SendStored() final;
  void SendSetSkipped() final;

  void StartArray(unsigned len);
  void SendEmptyArray();

  void SendVerbatimString(std::string_view str, VerbatimFormat format = TXT);

  static char* FormatDouble(double d, char* buf, unsigned len);
};

class ReqSerializer {
 public:
  explicit ReqSerializer(::io::Sink* stream) : sink_(stream) {
  }

  void SendCommand(std::string_view str);

  std::error_code ec() const {
    return ec_;
  }

 private:
  ::io::Sink* sink_;
  std::error_code ec_;
};

}  // namespace facade
