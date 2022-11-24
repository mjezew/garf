defmodule Garf.Graph do
  def query(graph_name, query) do
    Redix.command(:redix, ["GRAPH.QUERY", graph_name, query, "--compact"])
  end
end
