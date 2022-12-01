defmodule Garf.Graph.QueryResult do
  alias Garf.GraphCache
  defstruct [:stats, :header, :results]

  def parse([stats]) do
    %__MODULE__{stats: parse_stats(stats)}
  end

  def parse([header, results, stats]) do
    %__MODULE__{
      header: parse_header(header),
      results: parse_results(results),
      stats: parse_stats(stats)
    }
  end

  defp parse_header(header) do
    header
    |> Enum.map(fn [_column_type, header] -> header end)
  end

  defp parse_results(results) do
    results
    |> Enum.map(fn result ->
      Enum.map(result, fn
        [_is_edge = 7, [internal_id, type_id, source_node_id, destination_node_id, properties]] ->
          relationship = GraphCache.get_relationship(type_id)

          %{
            internal_id: internal_id,
            properties: build_edge(relationship, properties),
            source_node_id: source_node_id,
            destination_node_id: destination_node_id,
            relationship_type: relationship
          }

        # NOTE handle multiple label indexes when Redis supports it
        [_is_node = 8, [internal_id, [label_index], properties]] ->
          label = GraphCache.get_label(label_index)

          %{
            internal_id: internal_id,
            label: label,
            properties: build_node(label, properties)
          }

        _other ->
          nil
      end)
    end)
  end

  defp build_node(label, properties) do
    params =
      properties
      |> Enum.map(fn [property_index, _type, value] ->
        {GraphCache.get_property(property_index), value}
      end)
      |> Map.new()

    node_schema = GraphCache.get_node(label)
    node_schema.from_graph(params)
  end

  defp build_edge(relationship, properties) do
    params =
      properties
      |> Enum.map(fn [property_index, _type, value] ->
        {GraphCache.get_property(property_index), value}
      end)
      |> Map.new()

    edge_schema = GraphCache.get_edge(relationship)
    edge_schema.from_graph(params)
  end

  defp parse_stats(stats) do
    Enum.reduce(stats, %{}, fn
      "Labels added: " <> labels, acc ->
        Map.put(acc, :labels, labels)

      "Nodes created: " <> nodes, acc ->
        Map.put(acc, :nodes, nodes)

      "Properties set: " <> properties, acc ->
        Map.put(acc, :properties, properties)

      "Relationships created: " <> relationships, acc ->
        Map.put(acc, :relationships, relationships)

      "Query internal execution time: " <> execution_time, acc ->
        Map.put(acc, :execution_time, execution_time)

      "Cached execution: " <> cached_execution, acc ->
        Map.put(acc, :cached_execution, cached_execution)

      _other, acc ->
        acc
    end)
  end
end
