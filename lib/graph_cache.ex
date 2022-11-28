defmodule Garf.GraphCache do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_label(index) do
    case :ets.lookup(__MODULE__, {:labels, index}) do
      [{_key, value}] -> value
      # TODO: implement refresh
      [] -> nil
    end
  end

  def get_property(index) do
    case :ets.lookup(__MODULE__, {:property_keys, index}) do
      [{_key, value}] -> value
      [] -> nil
    end
  end

  def get_relationship(index) do
    case :ets.lookup(__MODULE__, {:relationship_types, index}) do
      [{_key, value}] -> value
      [] -> nil
    end
  end

  def init(opts) do
    :ets.new(__MODULE__, [:named_table, :protected, read_concurrency: true])
    {:ok, %{redix: opts[:redix]}, {:continue, :load_initial_cache}}
  end

  def handle_continue(:load_initial_cache, state) do
    with {:ok, state} <- refresh(:labels, state),
         {:ok, state} <- refresh(:relationship_types, state),
         {:ok, state} <- refresh(:property_keys, state) do
      {:noreply, state}
    else
      error ->
        error
    end
  end

  def refresh(identifier, %{redix: redix} = state) do
    with redis_identifier <- get_redis_identifier(identifier),
         {:ok, [[_identifier], labels, _stats]} <-
           Redix.command(redix, ["GRAPH.QUERY", "Garf", "CALL db.#{redis_identifier}()"]) do
      labels =
        labels
        |> Enum.with_index()
        |> Enum.map(fn {[label], index} -> {{identifier, index}, label} end)

      :ets.insert(__MODULE__, labels)

      {:ok, state}
    else
      error ->
        error
    end
  end

  defp get_redis_identifier(:labels), do: "labels"
  defp get_redis_identifier(:property_keys), do: "propertyKeys"
  defp get_redis_identifier(:relationship_types), do: "relationshipTypes"
end
