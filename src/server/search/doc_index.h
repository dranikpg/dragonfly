// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/mi_memory_resource.h"
#include "core/search/search.h"
#include "server/common.h"
#include "server/table.h"

namespace dfly {

using SearchDocData = absl::flat_hash_map<std::string /*field*/, std::string /*value*/>;

std::optional<search::SchemaField::FieldType> ParseSearchFieldType(std::string_view name);
std::string_view SearchFieldTypeToString(search::SchemaField::FieldType);

struct DocResult {
  struct SerializedValue {
    std::string key;
    SearchDocData values;
  };

  struct DocReference {
    ShardId shard_id;
    search::DocId doc_id;
    bool requested;
  };

  std::variant<SerializedValue, DocReference> value;
  search::ResultScore score;

  bool operator<(const DocResult& other) const;
  bool operator>=(const DocResult& other) const;
};

struct SearchResult {
  size_t write_epoch = 0;  // Write epoch of the index during on the result was created

  size_t total_hits = 0;        // total number of hits in shard
  std::vector<DocResult> docs;  // serialized documents of first hits

  // After combining results from multiple shards and accumulating more documents than initially
  // requested, only a subset of all documents will be sent back to the client,
  // so it doesn't make sense to serialize strictly all documents in every shard ahead.
  // Instead, only documents up to a probablistic bound are serialized, the
  // leftover ids and scores are stored in the cutoff tail for use in the "unlikely" scenario.
  // size_t num_cutoff = 0;

  std::optional<search::AlgorithmProfile> profile;
};

struct SearchParams {
  using FieldReturnList =
      std::vector<std::pair<std::string /*identifier*/, std::string /*short name*/>>;

  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset = 0;
  size_t limit_total = 10;

  // Total number of shards, used in probabilistic queries
  size_t num_shards = 0;
  bool enable_cutoff = false;

  // Set but empty means no fields should be returned
  std::optional<FieldReturnList> return_fields;
  std::optional<search::SortOption> sort_option;
  search::QueryParams query_params;

  bool IdsOnly() const {
    return return_fields && return_fields->empty();
  }

  bool ShouldReturnField(std::string_view field) const;
};

// Stores basic info about a document index.
struct DocIndex {
  enum DataType { HASH, JSON };

  // Get numeric OBJ_ code
  uint8_t GetObjCode() const;

  // Return true if the following document (key, obj_code) is tracked by this index.
  bool Matches(std::string_view key, unsigned obj_code) const;

  search::Schema schema;
  std::string prefix{};
  DataType type{HASH};
};

struct DocIndexInfo {
  DocIndex base_index;
  size_t num_docs = 0;

  // Build original ft.create command that can be used to re-create this index
  std::string BuildRestoreCommand() const;
};

class ShardDocIndices;

// Stores internal search indices for documents of a document index on a specific shard.
class ShardDocIndex {
  friend class ShardDocIndices;
  using DocId = search::DocId;

  // DocKeyIndex manages mapping document keys to ids and vice versa through a simple interface.
  struct DocKeyIndex {
    DocId Add(std::string_view key);
    DocId Remove(std::string_view key);

    std::string_view Get(DocId id) const;
    size_t Size() const;

   private:
    absl::flat_hash_map<std::string, DocId> ids_;
    std::vector<std::string> keys_;
    std::vector<DocId> free_ids_;
    DocId last_id_ = 0;
  };

 public:
  // Index must be rebuilt at least once after intialization
  ShardDocIndex(std::shared_ptr<DocIndex> index);

  // Perform search on all indexed documents and return results.
  io::Result<SearchResult, facade::ErrorReply> Search(const OpArgs& op_args,
                                                      const SearchParams& params,
                                                      search::SearchAlgorithm* search_algo) const;

  bool Refill(const OpArgs& op_args, const SearchParams& params,
              search::SearchAlgorithm* search_algo, SearchResult* result) const;

  // Perform search and load requested values - note params might be interpreted differently.
  std::vector<absl::flat_hash_map<std::string, search::SortableValue>> SearchForAggregator(
      const OpArgs& op_args, ArgSlice load_fields, search::SearchAlgorithm* search_algo) const;

  // Return whether base index matches
  bool Matches(std::string_view key, unsigned obj_code) const;

  void AddDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv);

  DocIndexInfo GetInfo() const;

 private:
  // Clears internal data. Traverses all matching documents and assigns ids.
  void Rebuild(const OpArgs& op_args, PMR_NS::memory_resource* mr);

  void Serialize(const OpArgs& op_args, const SearchParams& params,
                 absl::Span<DocResult> docs) const;

 private:
  std::shared_ptr<const DocIndex> base_;
  size_t write_epoch_;
  search::FieldIndices indices_;
  DocKeyIndex key_index_;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  ShardDocIndices();

  // Get sharded document index by its name or nullptr if not found
  ShardDocIndex* GetIndex(std::string_view name);

  // Init index: create shard local state for given index with given name.
  // Build if instance is in active state.
  void InitIndex(const OpArgs& op_args, std::string_view name, std::shared_ptr<DocIndex> index);

  // Drop index, return true if it existed and was dropped
  bool DropIndex(std::string_view name);

  // Rebuild all indices
  void RebuildAllIndices(const OpArgs& op_args);

  std::vector<std::string> GetIndexNames() const;

  void AddDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);
  void RemoveDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv);

  size_t GetUsedMemory() const;
  SearchStats GetStats() const;  // combines stats for all indices

 private:
  MiMemoryResource local_mr_;
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;
};

}  // namespace dfly
