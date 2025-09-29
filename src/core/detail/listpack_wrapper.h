#pragma once

#include <cstdint>
#include <cstdio>
#include <string_view>

#include "core/string_map.h"
#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
}

namespace dfly {

struct LpWrapper {
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<std::string_view, std::string_view>;

    Iterator() : lp{nullptr}, ptr{nullptr}, next_ptr{nullptr} {
    }

    explicit Iterator(uint8_t* lp) : Iterator{lp, lpFirst(lp)} {
      Read();
    }

    Iterator(uint8_t* lp, uint8_t* ptr) : lp{lp}, ptr{ptr}, next_ptr{nullptr} {
      Read();
    }

    Iterator& operator++() {
      ptr = next_ptr;
      Read();
      return *this;
    }

    value_type operator*() const {
      return {key_v, value_v};
    }

    value_type operator->() const {
      return {key_v, value_v};
    }

    bool operator==(const Iterator& other) const {
      return lp == other.lp && ptr == other.ptr;
    }

    bool operator!=(const Iterator& other) const {
      return !(operator==(other));
    }

    void Read() {
      if (!ptr)
        return;

      using container_utils::LpGetView;
      key_v = LpGetView(ptr, intbuf[0]);
      next_ptr = lpNext(lp, ptr);
      value_v = LpGetView(next_ptr, intbuf[1]);
      next_ptr = lpNext(lp, next_ptr);
    }

    uint8_t *lp, *ptr, *next_ptr;
    std::string_view key_v, value_v;
    uint8_t intbuf[LP_INTBUF_SIZE][2];  // TODO!
  };

  using const_iterator = Iterator;

  Iterator Find(std::string_view key) const {
    uint8_t* ptr = lpFind(lp, lpFirst(lp), (unsigned char*)key.data(), key.size(), 1);
    return Iterator{lp, ptr};
  }

  Iterator begin() const {
    return Iterator{lp};
  }

  Iterator end() const {
    return Iterator{};
  }

  size_t size() const {
    return lpLength((uint8_t*)lp) / 2;
  }

  uint8_t* lp;
};

struct HMapWrapper {
 private:
  template <typename F> decltype(auto) visit2(F f) {  // Cast T* to T&
    return std::visit(Overloaded{[&f](StringMap* s) { return f(*s); }, f}, hmap);
  }

 public:
  explicit HMapWrapper(const PrimeValue& pv, const DbContext& cntx) {
    if (pv.Encoding() == kEncodingListPack)
      hmap = LpWrapper{(uint8_t*)pv.RObjPtr()};
    else
      hmap = container_utils::GetStringMap(pv, cntx);
  }

  size_t Length() {
    return visit(Overloaded{
                     [](StringMap* s) { return s->UpperBoundSize(); },
                     [](LpWrapper lw) { return lw.size(); },
                 },
                 hmap);
  }

  auto Find(std::string_view key) {
    using RT = std::optional<std::pair<std::string_view, std::string_view>>;
    return visit2([key](auto& h) -> RT {
      if (auto it = h.Find(key); it != h.end())
        return *it;
      return std::nullopt;
    });
  }

  auto Range() {
    auto f = [](auto p) -> std::pair<std::string_view, std::string_view> { return p; };
    using IT = base::it::CompoundIterator<decltype(f), StringMap::iterator, LpWrapper::Iterator>;
    auto cb = [f](auto& h) -> std::pair<IT, IT> {
      return {{f, h.begin()}, {std::nullopt, h.end()}};
    };
    return base::it::Range(visit2(cb));
  }

 private:
  std::variant<StringMap*, LpWrapper> hmap;
};

}  // namespace dfly
