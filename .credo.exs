%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["lib/", "test/"],
        excluded: [~r"/_build/", ~r"/deps/"]
      },
      strict: true,
      checks: [
        {Credo.Check.Readability.ModuleDoc, false},
        {Credo.Check.Readability.Specs, false}
      ]
    }
  ]
}
