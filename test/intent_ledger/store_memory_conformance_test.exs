defmodule IntentLedger.StoreMemoryConformanceTest do
  use IntentLedger.StoreCase,
    async: false,
    store_module: IntentLedger.Store.Memory,
    store_opts: [name: __MODULE__.Store]

  use IntentLedger.StoreCase.AtomicCommitTests
  use IntentLedger.StoreCase.SemanticTests
end
