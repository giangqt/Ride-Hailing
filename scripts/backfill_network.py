"""
scripts/backfill_network.py
Batch reconstruction of the OD network over the full trip_events window.
Reproduces the Phase 4 Colab method (NetworkX betweenness + Louvain) that the
streaming network.py delegates downstream. Reads OD pair counts from trip_events,
builds a directed weighted graph, computes centrality, and writes:
  - network_flows      (top-N flows by trip_count, one time_window anchor)
  - network_centrality (per-zone degree/betweenness/pagerank/community)
Node centrality is attached to each flow's ORIGIN zone, matching existing rows.
"""

import os
import psycopg2
import psycopg2.extras
import pandas as pd
import networkx as nx
import community.community_louvain as louvain

PG = dict(host="localhost", port=5432, dbname="rides", user="rides", password=os.environ.get("PGPASSWORD", "rides"))
TOP_N_FLOWS = 500                       # rows written to network_flows
ANCHOR = "2026-06-10 00:00:00+00"       # inside dashboard window; single chunk

def main():
    conn = psycopg2.connect(**PG)
    conn.autocommit = False

    # 1. OD pair counts over the full window
    od = pd.read_sql(
        "SELECT pu_zone_id AS o, do_zone_id AS d, COUNT(*) AS trips "
        "FROM trip_events GROUP BY pu_zone_id, do_zone_id", conn)
    print(f"OD pairs: {len(od):,}  | zones: "
          f"{len(set(od.o) | set(od.d))}  | total trips: {od.trips.sum():,}")

    # 2. Directed weighted graph
    G = nx.DiGraph()
    for r in od.itertuples(index=False):
        G.add_edge(int(r.o), int(r.d), weight=int(r.trips))
    print(f"graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # 3. Centrality on the full graph
    #    betweenness weight = inverse trip_count (heavier flow = shorter path)
    for u, v, dta in G.edges(data=True):
        dta["distance"] = 1.0 / dta["weight"]
    betw = nx.betweenness_centrality(G, weight="distance", normalized=True)
    pr = nx.pagerank(G, weight="weight")
    indeg = dict(G.in_degree(weight="weight"))
    outdeg = dict(G.out_degree(weight="weight"))

    # 4. Louvain communities on the undirected projection
    comm = louvain.best_partition(G.to_undirected(), weight="weight", random_state=42)
    print(f"communities: {len(set(comm.values()))}")

    cur = conn.cursor()

    # 5. Refresh network_centrality (per zone)
    cur.execute("DELETE FROM network_centrality")
    cent_rows = [
        (ANCHOR, z, int(indeg.get(z, 0)), int(outdeg.get(z, 0)),
         float(betw.get(z, 0.0)), float(pr.get(z, 0.0)), int(comm.get(z, -1)))
        for z in G.nodes()
    ]
    psycopg2.extras.execute_values(cur,
        "INSERT INTO network_centrality "
        "(analysis_time, zone_id, in_degree, out_degree, betweenness, pagerank, community) "
        "VALUES %s", cent_rows)
    print(f"network_centrality: {len(cent_rows)} zones written")

    # 6. Refresh network_flows (top-N flows, origin-node centrality)
    cur.execute("DELETE FROM network_flows")
    top = od.sort_values("trips", ascending=False).head(TOP_N_FLOWS)
    flow_rows = [
        (ANCHOR, int(r.o), int(r.d), int(r.trips),
         float(indeg.get(int(r.o), 0)), float(outdeg.get(int(r.o), 0)),
         float(betw.get(int(r.o), 0.0)))
        for r in top.itertuples(index=False)
    ]
    psycopg2.extras.execute_values(cur,
        "INSERT INTO network_flows "
        "(time_window, origin_zone_id, dest_zone_id, trip_count, in_degree, out_degree, betweenness) "
        "VALUES %s", flow_rows)
    print(f"network_flows: {len(flow_rows)} flows written")

    conn.commit()
    cur.close()
    conn.close()
    print("done.")

if __name__ == "__main__":
    main()