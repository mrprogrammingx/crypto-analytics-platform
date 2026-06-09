#!/usr/bin/env python3
"""Export dbt's graph.gpickle to a PNG for recruiter-facing docs.

Usage:
  crypto_analytics_dbt/.venv/bin/python scripts/export_lineage_png.py

Writes: assets/lineage_graph.png
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DBT_TARGET = ROOT / "crypto_analytics_dbt" / "target"
GRAPH_FILE = DBT_TARGET / "graph.gpickle"
OUT_DIR = ROOT / "assets"
OUT_FILE = OUT_DIR / "lineage_graph.png"


def main():
    if not GRAPH_FILE.exists():
        print(f"graph.gpickle not found at {GRAPH_FILE}. Run `dbt docs generate` first.")
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    import argparse
    parser = argparse.ArgumentParser(description="Export dbt graph.gpickle to PNG with layout/dpi options")
    parser.add_argument("--out", default=str(OUT_FILE), help="output PNG path")
    parser.add_argument("--layout", choices=("dot", "spring"), default="dot", help="layout engine: 'dot' (graphviz) or 'spring' (force-directed)")
    parser.add_argument("--dpi", type=int, default=150, help="output DPI")
    parser.add_argument("--width", type=float, default=14.0, help="figure width in inches")
    parser.add_argument("--height", type=float, default=10.0, help="figure height in inches")
    parser.add_argument("--k", type=float, default=0.5, help="spring_layout 'k' parameter (only used with --layout spring)")
    parser.add_argument("--prog", default="dot", help="graphviz prog to use with dot layout (defaults to 'dot')")
    args = parser.parse_args()

    try:
        import networkx as nx
        import matplotlib.pyplot as plt
        import pickle
    except Exception:
        print("Missing dependencies. Install networkx and matplotlib in your venv:")
        print("  pip install networkx matplotlib")
        raise

    # networkx removed some top-level pickle helpers in newer releases; read the gpickle via pickle
    try:
        with open(GRAPH_FILE, "rb") as fh:
            G = pickle.load(fh)
    except Exception:
        # fallback to networkx convenience reader if available
        try:
            G = nx.read_gpickle(str(GRAPH_FILE))
        except Exception:
            print(f"Unable to read graph.gpickle at {GRAPH_FILE}")
            raise

    OUT_FILE_ACTUAL = Path(args.out)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Reduce to nodes that represent models (dbt uses nodes with package_name/model_type info)
    # The graph may include many internal nodes; try to pick nodes that look like models by checking 'resource_type' if present
    labels = {}
    nodes = []
    for n, data in G.nodes(data=True):
        # dbt's graph nodes often include a 'unique_id' or 'name' or 'resource_type'
        name = data.get("name") or data.get("unique_id") or str(n)
        rtype = data.get("resource_type")
        if rtype in ("model", "analysis", "seed", "snapshot") or "model" in name.lower() or "stg_" in name or "bronze_" in name:
            nodes.append(n)
            labels[n] = name

    # If we found no filtered nodes, fall back to the full graph
    if not nodes:
        nodes = list(G.nodes())
        labels = {n: (G.nodes[n].get("name") or str(n)) for n in nodes}

    H = G.subgraph(nodes).copy()

    plt.figure(figsize=(args.width, args.height))
    pos = None
    if args.layout == "spring":
        pos = nx.spring_layout(H, k=args.k)
    else:
        # try graphviz first, fall back to spring_layout
        try:
            pos = nx.nx_agraph.graphviz_layout(H, prog=args.prog)
        except Exception:
            pos = nx.spring_layout(H, k=args.k)

    nx.draw_networkx_edges(H, pos, arrowstyle="->", arrowsize=10, edge_color="#888")
    nx.draw_networkx_nodes(H, pos, node_size=400, node_color="#1f78b4")
    nx.draw_networkx_labels(H, pos, labels, font_size=8)

    plt.axis("off")
    plt.tight_layout()
    plt.savefig(OUT_FILE_ACTUAL, dpi=args.dpi)
    print(f"Wrote lineage PNG to {OUT_FILE_ACTUAL}")


if __name__ == "__main__":
    main()
