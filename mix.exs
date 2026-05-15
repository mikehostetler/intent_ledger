defmodule IntentLedger.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mikehostetler/intent_ledger"
  @description "OTP-native intent lifecycle ledger for deferred agent work"
  @extra_toc [
    {"README.md", [title: "Overview"]},
    {"guides/bedrock.md", [title: "Bedrock Runtime"]},
    {"guides/reliability.md", [title: "Reliability"]},
    {"guides/operations.md", [title: "Operations"]},
    {"guides/clustering.md", [title: "Clustering"]},
    {"CHANGELOG.md", [title: "Changelog"]},
    {"CONTRIBUTING.md", [title: "Contributing"]},
    {"LICENSE", [title: "License"]}
  ]
  @guide_paths for {path, _opts} <- @extra_toc, String.starts_with?(path, "guides/"), do: path
  @project_paths ["CHANGELOG.md", "CONTRIBUTING.md", "LICENSE"]

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
        summary: [threshold: 84],
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
        "test.fast": :test,
        "test.integration": :test,
        "test.bedrock": :test,
        "test.multi_node": :test,
        "test.chaos": :test
      ]
    ]
  end

  def application do
    [
      extra_applications: [:crypto, :logger]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support", "examples"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:jido_signal, "~> 2.1"},
      {:jason, "~> 1.4"},
      {:splode, "~> 0.3.0"},
      {:telemetry, "~> 1.0"},
      {:zoi, "~> 0.17.1"},

      # Bedrock runtime
      bedrock_deps(),

      # Dev/Test
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.22", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:excoveralls, "~> 0.18", only: [:dev, :test]},
      {:git_hooks, "~> 0.8", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.9", only: :dev, runtime: false}
    ]
    |> List.flatten()
  end

  defp bedrock_deps do
    if hex_build?() do
      [
        {:bedrock, "~> 0.5"},
        {:bedrock_job_queue, "~> 0.1"}
      ]
    else
      [
        {:bedrock, path: "../bedrock", override: true},
        {:bedrock_job_queue, path: "../job_queue"}
      ]
    end
  end

  defp hex_build?, do: "hex.build" in System.argv()

  defp aliases do
    [
      setup: ["deps.get"],
      install_hooks: ["git_hooks.install"],
      test: "test --exclude flaky --exclude integration --exclude bedrock --exclude multi_node --exclude chaos",
      "test.fast": "test --exclude flaky --exclude integration --exclude bedrock --exclude multi_node --exclude chaos",
      "test.integration": "test --exclude flaky --only integration",
      "test.bedrock": "test --exclude flaky --only bedrock --exclude multi_node",
      "test.multi_node": "test --exclude flaky --only multi_node",
      "test.chaos": "test --exclude flaky --only chaos",
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
      extras: @extra_toc,
      groups_for_extras: [
        Guides: @guide_paths,
        Project: @project_paths
      ],
      groups_for_modules: [
        "Public API": [
          IntentLedger,
          IntentLedger.Context,
          IntentLedger.Command,
          IntentLedger.Handler,
          IntentLedger.Intent,
          IntentLedger.Error,
          IntentLedger.Projection
        ],
        Runtime: [
          IntentLedger.BedrockStore,
          IntentLedger.Runtime,
          IntentLedger.Signal,
          IntentLedger.Telemetry
        ]
      ]
    ]
  end

  defp package do
    [
      name: :intent_ledger,
      files: [
        "lib",
        "mix.exs",
        ".formatter.exs",
        "README.md",
        "LICENSE",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "usage-rules.md",
        "guides"
      ],
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
