// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_builder.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/fixed_array.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <double-conversion/double-to-string.h>

#include "absl/strings/escaping.h"
#include "base/logging.h"
#include "core/heap_size.h"
#include "facade/error.h"
#include "util/fibers/proactor_base.h"

using namespace std;
using absl::StrAppend;
using namespace double_conversion;

namespace facade {

namespace {

inline iovec constexpr IoVec(std::string_view s) {
  iovec r{const_cast<char*>(s.data()), s.size()};
  return r;
}

constexpr char kCRLF[] = "\r\n";
constexpr char kErrPref[] = "-ERR ";
constexpr char kSimplePref[] = "+";
constexpr char kNullStringR2[] = "$-1\r\n";
constexpr char kNullStringR3[] = "_\r\n";

constexpr unsigned kConvFlags =
    DoubleToStringConverter::UNIQUE_ZERO | DoubleToStringConverter::EMIT_POSITIVE_EXPONENT_SIGN;

DoubleToStringConverter dfly_conv(kConvFlags, "inf", "nan", 'e', -6, 21, 6, 0);

}  // namespace

char* SinkReplyBuilder::ReservePiece(size_t size) {
  if (buffer_.AppendLen() <= size)
    Flush();

  char* dest = reinterpret_cast<char*>(buffer_.AppendBuffer().data());
  if (vecs_.empty() || (((char*)vecs_.back().iov_base) + vecs_.back().iov_len) != dest)
    NextVec({dest, 0});

  return dest;
}

void SinkReplyBuilder::CommitPiece(size_t size) {
  buffer_.CommitWrite(size);
  vecs_.back().iov_len += size;
  total_size_ += size;
}

void SinkReplyBuilder::WritePiece(std::string_view str) {
  char* dest = ReservePiece(str.size());
  memcpy(dest, str.data(), str.size());
  CommitPiece(str.size());
}

void SinkReplyBuilder::WriteRef(std::string_view str) {
  NextVec(str);
  ext_indices_.push_back(vecs_.size() - 1);
  total_size_ += str.size();
}

void SinkReplyBuilder::Flush() {
  // string out;
  // for (iovec v : vecs_)
  //   absl::StrAppend(&out, "{", string_view{(char*)v.iov_base, v.iov_len}, "}");
  // VLOG(0) << out;

  auto ec = sink_->Write(vecs_.data(), vecs_.size());
  if (ec)
    ec_ = ec;

  size_t buffer_bytes = buffer_.InputLen();

  buffer_.Clear();
  vecs_.clear();
  ext_indices_.clear();
  total_size_ = 0;

  if (buffer_bytes * 2 > buffer_.Capacity())
    buffer_.Reserve(std::min(kMaxBufferSize, buffer_.Capacity() * 2));
}

void SinkReplyBuilder::FinishScope() {
  if (!batched_ || total_size_ * 2 >= kMaxBufferSize)
    return Flush();

  size_t required = total_size_ - buffer_.InputLen();
  if (required > buffer_.AppendLen())
    return Flush();

  // Copy all extenral references to buffer to safely keep batching
  for (unsigned i : ext_indices_) {
    iovec& vec = vecs_[i];
    void* dest = buffer_.AppendBuffer().data();
    memcpy(dest, vec.iov_base, vec.iov_len);
    buffer_.CommitWrite(vec.iov_len);
    vec.iov_base = dest;
  }
  ext_indices_.clear();
}

void SinkReplyBuilder::NextVec(std::string_view str) {
  if (vecs_.size() >= IOV_MAX - 2)
    Flush();
  vecs_.push_back(iovec{const_cast<char*>(str.data()), str.size()});
}

void SinkReplyBuilder::SendError(ErrorReply error) {
  if (error.status)
    return SendError(*error.status);

  SendError(error.ToSv(), error.kind);
}

void SinkReplyBuilder::SendError(OpStatus status) {
  if (status == OpStatus::OK) {
    SendOk();
  } else {
    SendError(StatusToMsg(status));
  }
}

MCReplyBuilder::MCReplyBuilder(::io::Sink* sink) : SinkReplyBuilder(sink), noreply_(false) {
}

void MCReplyBuilder::SendValue(std::string_view key, std::string_view value, uint64_t mc_ver,
                               uint32_t mc_flag) {
  ReplyScope scope(this);
  Write("VALUE ", key, " ", absl::StrCat(mc_flag), " ", absl::StrCat(value.size()));
  if (mc_ver)
    Write(" ", absl::StrCat(mc_ver));
  Write(value, kCRLF);
}

void MCReplyBuilder::SendSimpleString(std::string_view str) {
  if (noreply_)
    return;

  ReplyScope scope(this);
  Write(str, kCRLF);
}

void MCReplyBuilder::SendStored() {
  SendSimpleString("STORED");
}

void MCReplyBuilder::SendLong(long val) {
  ReplyScope scope(this);
  char buf[32];
  char* next = absl::numbers_internal::FastIntToBuffer(val, buf);
  SendSimpleString(string_view(buf, next - buf));
}

void MCReplyBuilder::SendError(string_view str, std::string_view type) {
  SendSimpleString(absl::StrCat("SERVER_ERROR ", str));
}

void MCReplyBuilder::SendProtocolError(std::string_view str) {
  SendSimpleString(absl::StrCat("CLIENT_ERROR ", str));
}

bool MCReplyBuilder::NoReply() const {
  return noreply_;
}

void MCReplyBuilder::SendClientError(string_view str) {
  ReplyScope scope(this);
  Write("CLIENT_ERROR", str, kCRLF);
}

void MCReplyBuilder::SendSetSkipped() {
  SendSimpleString("NOT_STORED");
}

void MCReplyBuilder::SendNotFound() {
  SendSimpleString("NOT_FOUND");
}

void ReqSerializer::SendCommand(std::string_view str) {
  VLOG(2) << "SendCommand: " << str;

  iovec v[] = {IoVec(str), IoVec(kCRLF)};
  ec_ = sink_->Write(v, ABSL_ARRAYSIZE(v));
}

void RedisReplyBuilder2Base::SendNull() {
  ReplyScope scope(this);
  resp3_ ? Write(kNullStringR3) : Write(kNullStringR2);
}

void RedisReplyBuilder2Base::SendSimpleString(std::string_view str) {
  ReplyScope scope(this);
  Write(kSimplePref, str, kCRLF);
}

void RedisReplyBuilder2Base::SendBulkString(std::string_view str) {
  ReplyScope scope(this);
  WriteIntWithPrefix('$', str.size());
  Write(kCRLF, str, kCRLF);
}

void RedisReplyBuilder2Base::SendLong(long val) {
  ReplyScope scope(this);
  WriteIntWithPrefix(':', val);
  Write(kCRLF);
}

void RedisReplyBuilder2Base::SendDouble(double val) {
  char buf[DoubleToStringConverter::kBase10MaximalLength + 1];
  static_assert(ABSL_ARRAYSIZE(buf) < kMaxInlineSize, "Write temporary string from buf inline");

  StringBuilder sb(buf, ABSL_ARRAYSIZE(buf));
  dfly_conv.ToShortest(val, &sb);

  if (resp3_) {
    ReplyScope scope(this);
    Write(",", sb.Finalize(), kCRLF);
  } else {
    SendBulkString(sb.Finalize());
  }
}

void RedisReplyBuilder2Base::SendNullArray() {
  ReplyScope scope(this);
  Write("*-1", kCRLF);
}

constexpr static const char START_SYMBOLS[4][1] = {{'*'}, {'~'}, {'%'}, {'>'}};
static_assert(START_SYMBOLS[RedisReplyBuilder2Base::MAP][0] == '%' &&
              START_SYMBOLS[RedisReplyBuilder2Base::SET][0] == '~');

void RedisReplyBuilder2Base::StartCollection(unsigned len, CollectionType ct) {
  ReplyScope scope(this);
  if (!IsResp3()) {  // RESP2 supports only arrays
    if (ct == MAP)
      len *= 2;
    ct = ARRAY;
  }
  string_view prefix{START_SYMBOLS[ct], 1};
  WritePiece(absl::StrCat(prefix, len, kCRLF));
}

void RedisReplyBuilder2Base::WriteIntWithPrefix(char prefix, int64_t val) {
  char* dest = ReservePiece(absl::numbers_internal::kFastToBufferSize + 1);
  char* next = dest;
  *next++ = prefix;
  next = absl::numbers_internal::FastIntToBuffer(val, next);
  CommitPiece(next - dest);
}

void RedisReplyBuilder2Base::SendError(std::string_view str, std::string_view type) {
  ReplyScope scope(this);

  if (type.empty()) {
    type = str;
    if (type == kSyntaxErr)
      type = kSyntaxErrType;
  }
  tl_facade_stats->reply_stats.err_count[type]++;

  if (str[0] != '-')
    WritePiece(kErrPref);
  WritePiece(str);
  WritePiece(kCRLF);
}

void RedisReplyBuilder2Base::SendProtocolError(std::string_view str) {
  SendError(absl::StrCat("-ERR Protocol error: ", str), "protocol_error");
}

void RedisReplyBuilder::SendSimpleStrArr(const facade::ArgRange& strs) {
  ReplyScope scope(this);
  StartArray(strs.Size());
  for (std::string_view str : strs)
    SendSimpleString(str);
}

void RedisReplyBuilder::SendBulkStrArr(const facade::ArgRange& strs, CollectionType ct) {
  ReplyScope scope(this);
  StartCollection(strs.Size(), ct);
  for (std::string_view str : strs)
    SendBulkString(str);
}

void RedisReplyBuilder::SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
                                        bool with_scores) {
  ReplyScope scope(this);
  StartArray((with_scores && !IsResp3()) ? arr.size() * 2 : arr.size());
  for (const auto& [str, score] : arr) {
    if (IsResp3())
      StartArray(2);
    SendBulkString(str);
    if (with_scores)
      SendDouble(score);
  }
}

void RedisReplyBuilder::SendStored() {
  SendSimpleString("OK");
}

void RedisReplyBuilder::SendSetSkipped() {
  SendSimpleString("SKIPPED");
}

void RedisReplyBuilder::StartArray(unsigned len) {
  StartCollection(len, CollectionType::ARRAY);
}

void RedisReplyBuilder::SendEmptyArray() {
  StartArray(0);
}

void RedisReplyBuilder::SendVerbatimString(std::string_view str, VerbatimFormat format) {
  SendBulkString(str);
}

char* RedisReplyBuilder::FormatDouble(double d, char* buf, unsigned len) {
  return buf;
}

SinkReplyBuilder::ReplyAggregator::~ReplyAggregator() {
  if (!prev) {
    rb->batched_ = false;
    rb->Flush();
  }
}

SinkReplyBuilder::ReplyScope::~ReplyScope() {
  if (!prev) {
    rb->scoped_ = false;
    rb->FinishScope();
  }
}
}  // namespace facade
