// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cmd_support.h"

#include <absl/cleanup/cleanup.h>

#include "base/logging.h"
#include "server/command_registry.h"

namespace dfly::cmd {

void* CmdR::Coro::AllocFrame(size_t size, CommandContext* cmd) {
  void* ptr = cmd->TryInlineAlloc(size + 1);
  if (ptr) {
    static_cast<char*>(ptr)[size] = 1;
    return ptr;
  }
  ptr = ::operator new(size + 1);
  static_cast<char*>(ptr)[size] = 0;
  return ptr;
}

bool SingleHopWaiter::await_ready() noexcept {
  auto* tx = cmd_cntx->tx();

  if (!cmd_cntx->IsDeferredReply()) {
    // Use fiber blocking in synchronous mode
    tx->ScheduleSingleHop(callback);
    return true;
  } else {
    // Schedule async hop and keep transaction alive
    tx->SingleHopAsync(callback);
    // Multi transactions are kept alive by the squasher/exec context — skip the extra ref-count.
    if (!tx->IsMulti())
      tx_keepalive_ = tx;
    return false;
  }
}

void SingleHopWaiter::await_suspend(std::coroutine_handle<> handle) const noexcept {
  cmd_cntx->Resolve(cmd_cntx->tx()->Blocker(), handle);
}

facade::OpStatus SingleHopWaiter::await_resume() const noexcept {
  return *cmd_cntx->tx()->LocalResultPtr();
}

void CmdR::Coro::return_value(const facade::ErrorReply& err) const noexcept {
  cmd_cntx->SendError(err);
}

}  // namespace dfly::cmd
