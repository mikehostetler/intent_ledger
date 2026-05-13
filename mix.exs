defmodule IntentLedger.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mikehostetler/intent_ledger"
  @description "OTP-native intent lifecycle ledger for deferred agent work"

  def project do
    [
      app: :intent_ledger,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),

      # Docs
      name: "Intent Ledger",
      description: @description,
      source_url: @source_url,
      homepage_url: @source_url,
      package: package(),
      docs: docs(),

      # Test Coverage
      test_coverage: [
        tool: ExCoveralls,
        summary: [threshold: 90],
        export: "cov"
      ],

      # Dialyzer
      dialyzer: [
        plt_local_path: "priv/plts/project.plt",
        plt_core_path: "priv/plts/core.plt"
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.github": :test,
        "coveralls.html": :test,
        "coveralls.detail": :test,
        "test.integration": :test,
        "test.bedrock": :test,
        "test.multi_node": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger],
      mod: {IntentLedger.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:jido_signal, "~> 2.1"},
      {:jason, "~> 1.4"},
      {:splode, "~> 0.3.0"},
      {:telemetry, "~> 1.0"},
      {:zoi, "~> 0.17.1"},

      # Optional durable adapters
      {:bedrock, "~> 0.5.0", optional: true},

      # Dev/Test
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: [:dev, :test]},
      {:git_hooks, "~> 0.8", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.9", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get"],
      install_hooks: ["git_hooks.install"],
      test: "test --exclude flaky",
      "test.integration": "test --exclude flaky --only integration",
      "test.bedrock": "test --exclude flaky --only bedrock",
      "test.multi_node": "test --exclude flaky --only multi_node",
      q: ["quality"],
      quality: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --min-priority higher",
        "dialyzer",
        "doctor --raise"
      ]
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "docs/bedrock.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "LICENSE"
      ],
      groups_for_modules: [
        "Public API": [
          IntentLedger,
          IntentLedger.Command,
          IntentLedger.Intent,
          IntentLedger.IntentState,
          IntentLedger.Claim,
          IntentLedger.Claimed,
          IntentLedger.Error,
          IntentLedger.Record
        ],
        Runtime: [
          IntentLedger.Application,
          IntentLedger.Instance,
          IntentLedger.InstanceSupervisor,
          IntentLedger.Lifecycle,
          IntentLedger.Notifier,
          IntentLedger.QueueSupervisor,
          IntentLedger.QueueShardServer,
          IntentLedger.RecoveryServer,
          IntentLedger.SignalDispatcher,
          IntentLedger.SignalHandler,
          IntentLedger.Store,
          IntentLedger.Store.Commit,
          IntentLedger.Store.CommitRequest,
          IntentLedger.Store.Bedrock,
          IntentLedger.Store.Bedrock.Keyspace,
          IntentLedger.Store.Bedrock.Value,
          IntentLedger.Store.Conflict,
          IntentLedger.Store.Listing,
          IntentLedger.Store.Memory,
          IntentLedger.Store.Outbox,
          IntentLedger.Store.Precondition,
          IntentLedger.Store.Write
        ]
      ]
    ]
  end

  defp package do
    [
      name: :intent_ledger,
      files: ["lib", "mix.exs", "README.md", "LICENSE", "CHANGELOG.md", "usage-rules.md", "docs"],
      maintainers: ["Mike Hostetler"],
      licenses: ["Apache-2.0"],
      links: %{
        "Changelog" => "https://hexdocs.pm/intent_ledger/changelog.html",
        "Documentation" => "https://hexdocs.pm/intent_ledger",
        "GitHub" => @source_url
      }
    ]
  end
end
