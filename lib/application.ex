defmodule Garf.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Redix, host: "localhost", port: 6379, name: :redix},
      {Garf.GraphCache, redix: :redix}
    ]

    Supervisor.start_link(
      children,
      strategy: :one_for_one,
      name: Garf.Supervisor
    )
  end
end
