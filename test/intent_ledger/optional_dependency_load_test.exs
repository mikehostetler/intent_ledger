defmodule IntentLedger.OptionalDependencyLoadTest do
  use ExUnit.Case, async: false

  @moduletag timeout: 120_000

  @project_root Path.expand("../..", __DIR__)
  @optional_dep_dirs ["bedrock", "ecto_sql", "postgrex"]

  test "package compiles and loads without optional adapter dependencies" do
    tmp_dir = Path.join(System.tmp_dir!(), "intent_ledger_optional_load_#{System.unique_integer([:positive])}")

    File.rm_rf!(tmp_dir)
    File.mkdir_p!(Path.join(tmp_dir, "lib"))
    File.write!(Path.join(tmp_dir, "mix.exs"), mix_project_source(@project_root))
    File.write!(Path.join(tmp_dir, "lib/optional_dependency_probe.ex"), probe_source())

    try do
      assert_mix!(tmp_dir, ["deps.get"])

      for dep <- @optional_dep_dirs do
        refute File.dir?(Path.join([tmp_dir, "deps", dep]))
      end

      compile_output = assert_mix!(tmp_dir, ["compile", "--warnings-as-errors"])

      refute intent_ledger_compile_output(compile_output) =~ "warning:"

      assert_mix!(tmp_dir, ["run", "-e", "OptionalDependencyProbe.check!()"])
    after
      File.rm_rf!(tmp_dir)
    end
  end

  defp assert_mix!(cwd, args) do
    {output, status} =
      System.cmd("mix", args,
        cd: cwd,
        env: [{"MIX_ENV", "prod"}],
        stderr_to_stdout: true
      )

    assert status == 0, """
    mix #{Enum.join(args, " ")} failed with status #{status}

    #{output}
    """

    output
  end

  defp intent_ledger_compile_output(output) do
    output
    |> String.split("==> intent_ledger", parts: 2)
    |> case do
      [_all] ->
        ""

      [_before, rest] ->
        rest
        |> String.split("==> optional_dependency_probe", parts: 2)
        |> hd()
    end
  end

  defp mix_project_source(project_root) do
    """
    defmodule OptionalDependencyProbe.MixProject do
      use Mix.Project

      def project do
        [
          app: :optional_dependency_probe,
          version: "0.1.0",
          elixir: "~> 1.18",
          deps: deps()
        ]
      end

      def application do
        [
          extra_applications: [:logger]
        ]
      end

      defp deps do
        [
          {:intent_ledger, path: #{inspect(project_root)}, override: true}
        ]
      end
    end
    """
  end

  defp probe_source do
    """
    defmodule OptionalDependencyProbe do
      @optional_modules [
        Bedrock,
        Ecto,
        Ecto.Query,
        Ecto.Schema,
        Ecto.Migration,
        Ecto.Adapters.SQL,
        Ecto.Adapters.Postgres,
        Postgrex
      ]

      @intent_ledger_modules [
        IntentLedger,
        IntentLedger.Store.Memory,
        IntentLedger.Store.Bedrock,
        IntentLedger.Store.Bedrock.Keyspace,
        IntentLedger.Store.Bedrock.Value,
        IntentLedger.Store.Ecto,
        IntentLedger.Store.Ecto.Migration,
        IntentLedger.Store.Ecto.Query,
        IntentLedger.Store.Ecto.Schema
      ]

      def check! do
        Enum.each(@intent_ledger_modules, &Code.ensure_loaded!/1)

        for module <- @optional_modules do
          if Code.ensure_loaded?(module) do
            raise "\#{inspect(module)} should not load without an explicit optional dependency"
          end
        end

        if IntentLedger.Store.Bedrock.available?() do
          raise "Bedrock adapter should report unavailable without :bedrock"
        end

        if IntentLedger.Store.Ecto.available?() do
          raise "Ecto adapter should report unavailable without :ecto_sql and :postgrex"
        end

        :ok
      end
    end
    """
  end
end
