defmodule Garf.GraphCache do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    {:ok, %{redix: opts[:redix]}, {:continue, :load_initial_cache}}
  end

  def handle_continue(:load_initial_cache, state) do
    with {:ok, state} <- refresh(:labels, state),
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
           Redix.command(redix, ["GRAPH.QUERY", "MotoGP", "CALL db.#{redis_identifier}()"]) do
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
end
