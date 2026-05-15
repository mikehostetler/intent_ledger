defmodule IntentLedger.ConfigTest do
  use ExUnit.Case, async: true

  alias IntentLedger.Config
  alias IntentLedger.TestFixtures.{FailingIntent, SendInvoice}

  defmodule GenericHandler do
    @moduledoc false

    def __intent_handler__, do: %{topic: nil}
  end

  test "normalizes canonical intent definitions" do
    intents =
      Config.normalize_intents!(%{
        "invoice.send" => [handler: SendInvoice],
        "invoice.fail" => [handler: FailingIntent, queue: :critical, extra: true],
        "invoice.map" => [handler: GenericHandler, queue: "mapped"]
      })

    assert intents["invoice.send"].handler == SendInvoice
    assert intents["invoice.fail"].queue == "critical"
    assert intents["invoice.fail"].extra == true
    assert intents["invoice.map"].queue == "mapped"
    assert Config.handlers_from_intents(intents)["invoice.send"] == SendInvoice

    assert Config.handlers_from_intents!(%{"invoice.send" => [handler: SendInvoice]}) == %{
             "invoice.send" => SendInvoice
           }
  end

  test "normalizes queue definitions and defaults" do
    intents = Config.normalize_intents!(%{"invoice.send" => [handler: SendInvoice, queue: "billing"]})

    queues =
      Config.normalize_queues!(
        %{
          :default => %{},
          "bulk" => [concurrency: 10],
          "json" => %{priority: :high}
        },
        intents
      )

    assert Config.queue_ids(queues) == ["billing", "bulk", "default", "json"]
    assert queues["bulk"].concurrency == 10
    assert queues["json"].priority == :high
    assert Config.normalize_default_queue!(nil, queues) == "default"
    assert Config.normalize_default_queue!(:billing, queues) == "billing"
  end

  test "adds a default queue when no explicit or intent queue exists" do
    intents = Config.normalize_intents!(%{"invoice.send" => [handler: SendInvoice]})

    assert Config.normalize_queues!(nil, intents) == %{"default" => %{id: "default"}}
  end

  test "validates invalid config inputs" do
    assert_raise ArgumentError, "IntentLedger requires :intents", fn -> Config.normalize_intents!(nil) end
    assert_raise ArgumentError, "IntentLedger requires at least one intent", fn -> Config.normalize_intents!([]) end
    assert_raise ArgumentError, fn -> Config.normalize_intents!(:bad) end
    assert_raise ArgumentError, fn -> Config.normalize_intents!(%{"invoice.send" => nil}) end
    assert_raise ArgumentError, fn -> Config.normalize_intents!(%{"invoice.mismatch" => [handler: SendInvoice]}) end
    assert_raise ArgumentError, fn -> Config.normalize_queues!(:bad) end
    assert_raise ArgumentError, fn -> Config.normalize_queues!([:default, "default"]) end
    assert_raise ArgumentError, fn -> Config.normalize_queues!([%{}]) end
    assert_raise ArgumentError, fn -> Config.normalize_default_queue!("missing", %{"default" => %{id: "default"}}) end

    assert Config.normalize_topic("") == {:error, {:invalid_topic, ""}}
    assert Config.normalize_topic(123) == {:error, {:invalid_topic, 123}}
    assert Config.normalize_queue_id("") == {:error, {:invalid_queue, ""}}
    assert Config.normalize_queue_id(123) == {:error, {:invalid_queue, 123}}
  end
end
