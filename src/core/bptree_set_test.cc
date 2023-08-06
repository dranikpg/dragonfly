// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/bptree_set.h"

#include <mimalloc.h>

#include <random>

extern "C" {
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

#include "base/gtest.h"
#include "base/init.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

using namespace std;

namespace dfly {

class BPTreeSetTest : public ::testing::Test {
  using Node = detail::BPTreeNode<uint64_t>;

 protected:
  BPTreeSetTest() : mi_alloc_(mi_heap_get_backing()), bptree_(&mi_alloc_) {
  }
  static void SetUpTestSuite() {
  }

  bool Validate();

  static bool Validate(const Node* node, uint64_t ubound);

  MiMemoryResource mi_alloc_;
  BPTree<uint64_t> bptree_;
  mt19937 generator_{1};
};

bool BPTreeSetTest::Validate(const Node* node, uint64_t ubound) {
  for (unsigned i = 1; i < node->NumItems(); ++i) {
    if (node->Key(i - 1) >= node->Key(i))
      return false;
  }

  if (!node->IsLeaf()) {
    unsigned mask = 0;
    uint32_t subtree_cnt = node->NumItems();
    for (unsigned i = 0; i <= node->NumItems(); ++i) {
      mask |= (1 << node->Child(i)->IsLeaf());
      DCHECK_EQ(node->Child(i)->DEBUG_TreeCount(), node->Child(i)->TreeCount());
      subtree_cnt += node->Child(i)->TreeCount();
    }
    if (mask == 3)
      return false;

    if (subtree_cnt != node->TreeCount()) {
      LOG(ERROR) << "Expected " << subtree_cnt << " got " << node->TreeCount();
      return false;
    }
  }

  return node->Key(node->NumItems() - 1) < ubound;
}

bool BPTreeSetTest::Validate() {
  auto* root = bptree_.DEBUG_root();
  if (!root)
    return true;

  // node, upper bound
  std::vector<pair<Node*, uint64_t>> stack;

  stack.emplace_back(root, UINT64_MAX);

  while (!stack.empty()) {
    Node* node = stack.back().first;
    uint64_t ubound = stack.back().second;
    stack.pop_back();

    if (!Validate(node, ubound))
      return false;

    if (!node->IsLeaf()) {
      for (unsigned i = 0; i < node->NumItems(); ++i) {
        stack.emplace_back(node->Child(i), node->Key(i));
      }
      stack.emplace_back(node->Child(node->NumItems()), ubound);
    }
  }
  return true;
}

TEST_F(BPTreeSetTest, BPtreeInsert) {
  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bptree_.Insert(i));
    ASSERT_EQ(i, bptree_.Size());
    ASSERT_EQ(i - 1, bptree_.GetRank(i));
    // ASSERT_TRUE(Validate()) << i;
  }
  ASSERT_TRUE(Validate());

  ASSERT_GT(mi_alloc_.used(), 56000u);
  ASSERT_LT(mi_alloc_.used(), 66000u);

  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bptree_.Contains(i));
  }

  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  uniform_int_distribution<uint64_t> dist(0, 100000);
  for (unsigned i = 0; i < 20000; ++i) {
    bptree_.Insert(dist(generator_));
    // ASSERT_TRUE(Validate()) << i;
  }
  ASSERT_TRUE(Validate());
  ASSERT_GT(mi_alloc_.used(), 10000u);
  LOG(INFO) << bptree_.Height() << " " << bptree_.Size();

  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  for (unsigned i = 20000; i > 1; --i) {
    bptree_.Insert(i);
  }
  ASSERT_TRUE(Validate());
  for (unsigned i = 2; i <= 20000; ++i) {
    ASSERT_EQ(i - 2, bptree_.GetRank(i));
  }

  LOG(INFO) << bptree_.Height() << " " << bptree_.Size();
  ASSERT_GT(mi_alloc_.used(), 20000 * 8);
  ASSERT_LT(mi_alloc_.used(), 20000 * 10);
  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);
}

TEST_F(BPTreeSetTest, Delete) {
  for (unsigned i = 31; i > 10; --i) {
    bptree_.Insert(i);
  }

  for (unsigned i = 1; i < 10; ++i) {
    ASSERT_FALSE(bptree_.Delete(i));
  }

  for (unsigned i = 11; i < 32; ++i) {
    ASSERT_TRUE(bptree_.Delete(i));
  }
  ASSERT_EQ(mi_alloc_.used(), 0u);
  ASSERT_EQ(bptree_.Size(), 0u);

  constexpr size_t kNumElems = 7000;
  for (unsigned i = 0; i < kNumElems; ++i) {
    bptree_.Insert(i);
  }

  ASSERT_GT(bptree_.NodeCount(), 2u);
  unsigned sz = bptree_.Size();
  for (unsigned i = 0; i < kNumElems; ++i) {
    --sz;
    ASSERT_EQ(bptree_.GetRank(kNumElems - 1), sz);

    ASSERT_TRUE(bptree_.Delete(i));
    ASSERT_EQ(bptree_.Size(), sz);
    // ASSERT_TRUE(Validate()) << i;
  }

  ASSERT_EQ(mi_alloc_.used(), 0u);
  ASSERT_EQ(bptree_.Size(), 0u);
  ASSERT_EQ(bptree_.Height(), 0u);
  ASSERT_EQ(bptree_.NodeCount(), 0u);
}

TEST_F(BPTreeSetTest, Iterate) {
  constexpr size_t kNumElems = 7000;
  for (unsigned i = 0; i < kNumElems; ++i) {
    bptree_.Insert(i * 2);
  }

  unsigned cnt = 0;
  bptree_.Iterate(31, 543, [&](uint64_t val) {
    ASSERT_EQ((31 + cnt) * 2, val);
    ++cnt;
  });
  ASSERT_EQ(543 - 31 + 1, cnt);

  for (unsigned j = 0; j < 10; ++j) {
    cnt = 0;
    unsigned from = generator_() % kNumElems;
    unsigned to = from + generator_() % (kNumElems - from);
    bptree_.Iterate(from, to, [&](uint64_t val) {
      ASSERT_EQ((from + cnt) * 2, val) << from << " " << to << " " << cnt;
      ++cnt;
    });
    ASSERT_EQ(to - from + 1, cnt);
  }

  // Reverse iteration
  cnt = 0;
  bptree_.IterateReverse(5845, 6849, [&](uint64_t val) {
    ASSERT_EQ((6849 - cnt) * 2, val);
    ++cnt;
  });
  ASSERT_EQ(6849 - 5845 + 1, cnt);

  for (unsigned j = 0; j < 10; ++j) {
    cnt = 0;
    unsigned from = generator_() % kNumElems;
    unsigned to = from + generator_() % (kNumElems - from);
    bptree_.IterateReverse(from, to, [&](uint64_t val) {
      ASSERT_EQ((to - cnt) * 2, val) << from << " " << to << " " << cnt;
      ++cnt;
    });
    ASSERT_EQ(to - from + 1, cnt);
  }
}

struct ZsetPolicy {
  struct KeyT {
    double d;
    sds s;
  };

  struct KeyCompareTo {
    int operator()(const KeyT& left, const KeyT& right) {
      if (left.d < right.d)
        return -1;
      if (left.d > right.d)
        return 1;

      return sdscmp(left.s, right.s);
    }
  };
};

using SDSTree = BPTree<ZsetPolicy::KeyT, ZsetPolicy>;

static string RandomString(mt19937& rand, unsigned len) {
  const string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  string ret;
  ret.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    ret += alpanum[rand() % alpanum.size()];
  }

  return ret;
}

std::vector<ZsetPolicy::KeyT> GenerateRandomPairs(unsigned len) {
  mt19937 dre(10);
  std::vector<ZsetPolicy::KeyT> vals(len, ZsetPolicy::KeyT{});
  for (unsigned i = 0; i < len; ++i) {
    vals[i].d = dre();
    vals[i].s = sdsnew(RandomString(dre, 10).c_str());
  }
  return vals;
}

static void BM_FindRandomBPTree(benchmark::State& state) {
  unsigned iters = state.range(0);
  std::vector<ZsetPolicy::KeyT> vals = GenerateRandomPairs(iters);
  SDSTree bptree;
  for (unsigned i = 0; i < iters; ++i) {
    bptree.Insert(vals[i]);
  }

  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      benchmark::DoNotOptimize(bptree.Contains(vals[i]));
    }
  }
  for (const auto v : vals) {
    sdsfree(v.s);
  }
}
BENCHMARK(BM_FindRandomBPTree)->Arg(1024)->Arg(1 << 16)->Arg(1 << 20);

static void BM_FindRandomZSL(benchmark::State& state) {
  zskiplist* zsl = zslCreate();
  unsigned iters = state.range(0);
  std::vector<ZsetPolicy::KeyT> vals = GenerateRandomPairs(iters);
  for (unsigned i = 0; i < iters; ++i) {
    zslInsert(zsl, vals[i].d, sdsdup(vals[i].s));
  }

  while (state.KeepRunning()) {
    for (unsigned i = 0; i < iters; ++i) {
      benchmark::DoNotOptimize(zslGetRank(zsl, vals[i].d, vals[i].s));
    }
  }

  zslFree(zsl);

  for (const auto v : vals) {
    sdsfree(v.s);
  }
}
BENCHMARK(BM_FindRandomZSL)->Arg(1024)->Arg(1 << 16)->Arg(1 << 20);

void RegisterBPTreeBench() {
  auto* tlh = mi_heap_get_backing();
  init_zmalloc_threadlocal(tlh);
};

REGISTER_MODULE_INITIALIZER(Bptree, RegisterBPTreeBench());

}  // namespace dfly