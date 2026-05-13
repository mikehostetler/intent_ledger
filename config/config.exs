import Config

config :git_hooks,
  auto_install: Mix.env() == :dev,
  verbose: true,
  hooks: [
    pre_commit: [
      tasks: [
        {:cmd, "mix format --check-formatted"},
        {:cmd, "mix compile --warnings-as-errors"},
        {:cmd, "mix test"}
      ]
    ],
    pre_push: [
      tasks: [
        {:cmd, "mix quality"}
      ]
    ]
  ]

import_config "#{config_env()}.exs"
