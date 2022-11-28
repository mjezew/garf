defmodule Garf.Graph do
  alias Garf.Graph.QueryResult

  def query(graph_name, query) do
    case Redix.command(:redix, ["GRAPH.QUERY", graph_name, query, "--compact"]) do
      {:ok, result} -> {:ok, QueryResult.parse(result)}
      error -> error
    end
  end
end
