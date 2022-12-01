defmodule Garf.GraphCache do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_label(index) do
    cache_lookup(:labels, index)
  end

  def get_property(index) do
    cache_lookup(:property_keys, index)
  end

  def get_relationship(index) do
    cache_lookup(:relationship_types, index)
  end

  def get_node(key) do
    [{_key, nodes}] = :ets.lookup(__MODULE__, :nodes)
    Map.get(nodes, key)
  end

  def get_edge(key) do
    [{_key, edges}] = :ets.lookup(__MODULE__, :edges)
    Map.get(edges, key)
  end

  defp cache_lookup(identifier, index) do
    case :ets.lookup(__MODULE__, {identifier, index}) do
      [{_key, value}] -> value
      [] -> GenServer.call(__MODULE__, {:refresh, identifier, index})
    end
  end

  def init(opts) do
    :ets.new(__MODULE__, [:named_table, :protected, read_concurrency: true])
    :ets.insert(__MODULE__, {:nodes, opts[:nodes]})
    :ets.insert(__MODULE__, {:edges, opts[:edges]})
    {:ok, %{redix: opts[:redix]}, {:continue, :load_initial_cache}}
  end

  def handle_continue(:load_initial_cache, state) do
    with :ok <- refresh(:labels, state),
         :ok <- refresh(:relationship_types, state),
         :ok <- refresh(:property_keys, state) do
      {:noreply, state}
    else
      error ->
        error
    end
  end

  def handle_call({:refresh, identifier, index}, _caller, state) do
    case :ets.lookup(__MODULE__, {identifier, index}) do
      [] ->
        refresh(identifier, state)
        {:reply, cache_lookup(identifier, index), state}

      [{_key, value}] ->
        {:reply, value, state}
    end
  end

  def refresh(identifier, %{redix: redix}) do
    redis_identifier = get_redis_identifier(identifier)

    {:ok, [[_identifier], labels, _stats]} =
      Redix.command(redix, ["GRAPH.QUERY", "Garf", "CALL db.#{redis_identifier}()"])

    labels =
      labels
      |> Enum.with_index()
      |> Enum.map(fn {[label], index} -> {{identifier, index}, label} end)

    :ets.insert(__MODULE__, labels)

    :ok
  end

  defp get_redis_identifier(:labels), do: "labels"
  defp get_redis_identifier(:property_keys), do: "propertyKeys"
  defp get_redis_identifier(:relationship_types), do: "relationshipTypes"
end
