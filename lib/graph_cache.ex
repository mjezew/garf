defmodule Garf.GraphCache do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_label(index) do
    GenServer.call(__MODULE__, {:get_label, index})
  end

  def get_property(index) do
    GenServer.call(__MODULE__, {:get_property, index})
  end

  def get_relationship(index) do
    GenServer.call(__MODULE__, {:get_relationship, index})
  end

  def init(opts) do
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

  # TODO: fetch label if not in cache
  def handle_call({:get_label, index}, _caller, %{labels: labels} = state) do
    {:reply, Map.get(labels, index), state}
  end

  def handle_call({:get_property, index}, _caller, %{property_keys: properties} = state) do
    {:reply, Map.get(properties, index), state}
  end

  def handle_call(
        {:get_relationship, index},
        _caller,
        %{relationship_types: relationship_types} = state
      ) do
    {:reply, Map.get(relationship_types, index), state}
  end

  def refresh(identifier, %{redix: redix} = state) do
    with redis_identifier <- get_redis_identifier(identifier),
         {:ok, [[_identifier], labels, _stats]} <-
           Redix.command(redix, ["GRAPH.QUERY", "Garf", "CALL db.#{redis_identifier}()"]) do
      labels =
        labels
        |> Enum.with_index()
        |> Enum.reduce(%{}, fn {[label], index}, acc -> Map.put(acc, index, label) end)

      state = Map.put(state, identifier, labels)
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
