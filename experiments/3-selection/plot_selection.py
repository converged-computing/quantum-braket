#!/usr/bin/env python3
"""
plot_selection.py — visualize cost/queue selection results.

Reads a selection-*.csv from results/ and renders, into img/:
  - cost experiment:  per-run cost scatter + mean bar, baseline vs min-cost,
    with the backend each run landed on annotated (shows baseline's spread /
    chance-expensive picks vs min-cost's flat floor).
  - queue experiment: per-run queue-at-submit + chosen backend, baseline vs
    min-queue (illustrative; queue is exogenous).

Usage:
  python3 plot_selection.py                       # newest selection-*.csv
  python3 plot_selection.py results/selection-cost-....csv
"""
import argparse
import csv
import glob
import os
import statistics as st
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

COLORS = {"baseline": "#6C5B9E", "min-cost": "#0E7C86", "min-queue": "#0E7C86"}
HERE = os.path.dirname(os.path.abspath(__file__))
IMG = os.path.join(HERE, "img")


def load(path):
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows


def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _cost_series(rows):
    arms = ["baseline", "min-cost"]
    data = {a: [] for a in arms}
    landed = {a: {} for a in arms}
    for r in rows:
        a = r["arm"]
        if a not in data:
            continue
        c = fnum(r.get("realized_cost_usd"))
        if c is not None:
            data[a].append(c)
        b = r.get("realized_backend") or "?"
        landed[a][b] = landed[a].get(b, 0) + 1
    return arms, data, landed


def _queue_series(rows, metric="runtime"):
    arms = ["baseline", "min-queue"]
    key = {"depth": "queue_at_submit",
           "wait": "qpu_queue_wait_s",
           "runtime": "producer_wall_s"}.get(metric, "producer_wall_s")
    qd = {a: [] for a in arms}
    landed = {a: {} for a in arms}
    for r in rows:
        a = r["arm"]
        if a not in qd:
            continue
        v = fnum(r.get(key))
        if v is not None:
            qd[a].append(v)
        b = r.get("realized_backend") or "?"
        landed[a][b] = landed[a].get(b, 0) + 1
    return arms, qd, landed


def _panel_perrun_cost(ax, arms, data, rng):
    for i, a in enumerate(arms):
        v = data[a]
        if not v:
            continue
        xs = i + rng.uniform(-0.18, 0.18, size=len(v))
        ax.scatter(xs, v, color=COLORS[a], s=44, zorder=3, edgecolor="white", linewidth=0.6)
        ax.plot([i - 0.25, i + 0.25], [st.median(v)] * 2, color="black", lw=1.6, zorder=4)
    ax.set_xticks(range(len(arms)))
    ax.set_xticklabels(arms)
    ax.set_xlim(-0.6, len(arms) - 0.4)
    ax.set_ylabel("realized cost per run (USD)")
    ax.set_ylim(bottom=0)
    ax.set_title("Per-run cost  (bar = median)", fontsize=11)
    ax.grid(axis="y", alpha=0.3)


def _panel_mean_cost(ax, arms, data, landed):
    for i, a in enumerate(arms):
        v = data[a]
        if not v:
            continue
        m = st.mean(v)
        s = st.stdev(v) if len(v) > 1 else 0
        ax.bar(i, m, width=0.55, yerr=s, capsize=5, color=COLORS[a], alpha=0.85,
               error_kw=dict(ecolor="black", lw=1.2))
        ax.text(i, m + s, f"${m:.3f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
        items = [f"{k}×{n}" for k, n in sorted(landed[a].items())]
        bd = "\n".join(items)
        ax.annotate(bd, xy=(i, 0), xycoords=("data", "axes fraction"),
                    xytext=(0, -26), textcoords="offset points",
                    ha="center", va="top", fontsize=6, color=COLORS[a])
    ax.set_xticks(range(len(arms)))
    ax.set_xticklabels(arms)
    ax.set_xlim(-0.6, len(arms) - 0.4)
    ax.set_ylabel("mean cost per run (USD)")
    ax.set_ylim(bottom=0)
    ax.set_title("Mean cost (± stdev)", fontsize=11)
    ax.grid(axis="y", alpha=0.3)


def _panel_queue(ax, arms, qd, landed, rng, metric="runtime"):
    logscale = (metric in ("wait", "runtime"))
    for i, a in enumerate(arms):
        v = qd[a]
        if not v:
            continue
        xs = i + rng.uniform(-0.16, 0.16, size=len(v))
        yv = [max(x, 1.0) for x in v] if logscale else v   # avoid log(0)
        ax.scatter(xs, yv, color=COLORS[a], s=50, zorder=3, edgecolor="white", linewidth=0.6)
        med = st.median(yv)
        ax.plot([i - 0.25, i + 0.25], [med] * 2, color="black", lw=1.6, zorder=4)
    ax.set_xticks(range(len(arms)))
    ax.set_xticklabels(arms)
    ax.set_xlim(-0.6, len(arms) - 0.4)
    # backend annotations sit just under the axis
    ymin = 0.7 if logscale else 0
    for i, a in enumerate(arms):
        items = [f"{k}×{n}" for k, n in sorted(landed[a].items())]
        bd = "\n".join(items)
        ax.annotate(bd, xy=(i, 0), xycoords=("data", "axes fraction"),
                    xytext=(0, -22), textcoords="offset points",
                    ha="center", va="top", fontsize=6, color=COLORS[a])
    if logscale:
        ax.set_yscale("log")
        if metric == "runtime":
            ax.set_ylabel("time to result (s, log)")
            ax.set_title("Time to result", fontsize=11)
        else:
            ax.set_ylabel("queue wait at submit (s, log)")
            ax.set_title("Queue wait", fontsize=11)
    else:
        ax.set_ylim(bottom=0)
        ax.set_ylabel("chosen device queue depth at submit")
        ax.set_title("Queue depth at submit", fontsize=11)
    ax.grid(axis="y", alpha=0.3)


def plot_combined(cost_rows, queue_rows, out, layout="row", queue_metric="runtime"):
    """One 2-panel figure: mean cost | queue. layout='row' (1x2) or 'col' (2x1).
    queue_metric='depth' (queue_at_submit, committed) or 'wait'
    (realized qpu_queue_wait_s, log scale)."""
    c_arms, c_data, c_landed = _cost_series(cost_rows)
    q_arms, q_data, q_landed = _queue_series(queue_rows, queue_metric)
    rng = np.random.default_rng(0)
    if layout == "col":
        fig, axes = plt.subplots(2, 1, figsize=(4.4, 8.0))
    else:
        fig, axes = plt.subplots(1, 2, figsize=(8.0, 4.4))
        fig.subplots_adjust(wspace=0.34)
    _panel_mean_cost(axes[0], c_arms, c_data, c_landed)
    _panel_queue(axes[1], q_arms, q_data, q_landed, rng, queue_metric)

    n_cost = max((len(c_data[a]) for a in c_arms), default=0)
    n_q = max((len(q_data[a]) for a in q_arms), default=0)
    fig.suptitle("Cost- and queue-aware selection", fontsize=12, fontweight="bold")
    rect = [0, 0.0, 1, 0.94] if layout == "row" else [0, 0.0, 1, 0.965]
    fig.tight_layout(rect=rect)
    for ext in ("png", "svg"):
        p = os.path.splitext(out)[0] + "." + ext
        fig.savefig(p, dpi=150, bbox_inches="tight")
        print(f"wrote {p}")
    plt.close(fig)


def plot_cost(rows, out):
    arms = ["baseline", "min-cost"]
    data = {a: [] for a in arms}
    landed = {a: {} for a in arms}
    for r in rows:
        a = r["arm"]
        if a not in data:
            continue
        c = fnum(r.get("realized_cost_usd"))
        if c is not None:
            data[a].append(c)
        b = r.get("realized_backend") or "?"
        landed[a][b] = landed[a].get(b, 0) + 1
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.8))
    rng = np.random.default_rng(0)
    for i, a in enumerate(arms):
        v = data[a]
        if not v:
            continue
        xs = i + rng.uniform(-0.18, 0.18, size=len(v))
        ax1.scatter(xs, v, color=COLORS[a], s=44, zorder=3, edgecolor="white", linewidth=0.6)
        ax1.plot([i - 0.25, i + 0.25], [st.median(v)] * 2, color="black", lw=1.6, zorder=4)
    ax1.set_xticks(range(len(arms)))
    ax1.set_xticklabels(arms)
    ax1.set_ylabel("realized cost per run (USD)")
    ax1.set_ylim(bottom=0)
    ax1.set_title("Per-run cost  (point = run; bar = median)")
    ax1.grid(axis="y", alpha=0.3)

    for i, a in enumerate(arms):
        v = data[a]
        if not v:
            continue
        m = st.mean(v)
        s = st.stdev(v) if len(v) > 1 else 0
        ax2.bar(i, m, width=0.55, yerr=s, capsize=5, color=COLORS[a], alpha=0.85,
                error_kw=dict(ecolor="black", lw=1.2))
        ax2.text(i, m + s, f"${m:.3f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
        # annotate which backends each arm landed on
        bd = ", ".join(f"{k}×{n}" for k, n in sorted(landed[a].items()))
        ax2.text(i, -0.06 * (ax2.get_ylim()[1] or 1), bd, ha="center", va="top",
                 fontsize=7.5, color=COLORS[a], rotation=0)
    ax2.set_xticks(range(len(arms)))
    ax2.set_xticklabels(arms)
    ax2.set_ylabel("mean cost per run (USD)")
    ax2.set_ylim(bottom=0)
    ax2.set_title("Mean cost  (error bars = stdev)")
    ax2.grid(axis="y", alpha=0.3)

    nrep = max((len(data[a]) for a in arms), default=0)
    fig.suptitle("Cost-aware selection vs random baseline", fontsize=13, fontweight="bold")
    fig.text(0.5, -0.02,
             f"n={nrep}/arm . baseline picks randomly from the same candidate pool "
             f"(may land on an expensive QPU by chance); "
             f"min-cost pins the cheapest satisfying backend",
             ha="center", fontsize=8.5, style="italic")
    fig.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


def plot_queue(rows, out):
    arms = ["baseline", "min-queue"]
    qd = {a: [] for a in arms}
    landed = {a: {} for a in arms}
    for r in rows:
        a = r["arm"]
        if a not in qd:
            continue
        q = fnum(r.get("queue_at_submit"))
        if q is not None:
            qd[a].append(q)
        b = r.get("realized_backend") or "?"
        landed[a][b] = landed[a].get(b, 0) + 1
    fig, ax = plt.subplots(figsize=(7.5, 4.8))
    rng = np.random.default_rng(0)
    for i, a in enumerate(arms):
        v = qd[a]
        if not v:
            continue
        xs = i + rng.uniform(-0.16, 0.16, size=len(v))
        ax.scatter(xs, v, color=COLORS[a], s=50, zorder=3, edgecolor="white", linewidth=0.6)
        ax.plot([i - 0.25, i + 0.25], [st.median(v)] * 2, color="black", lw=1.6, zorder=4)
        bd = ", ".join(f"{k}×{n}" for k, n in sorted(landed[a].items()))
        ax.text(i, -0.06 * (ax.get_ylim()[1] or 1), bd, ha="center", va="top",
                fontsize=8, color=COLORS[a])
    ax.set_xticks(range(len(arms)))
    ax.set_xticklabels(arms)
    ax.set_ylabel("chosen device queue depth at submit")
    ax.set_ylim(bottom=0)
    ax.set_title("Queue-aware selection (illustrative; queue is exogenous)")
    ax.grid(axis="y", alpha=0.3)
    fig.text(0.5, -0.02,
             "single, uncontrolled observations: min-queue pins the shortest-queue "
             "online device at submit time",
             ha="center", fontsize=8.5, style="italic")
    fig.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


def _newest(kind):
    c = sorted(glob.glob(os.path.join(HERE, "results", f"selection-{kind}-*.csv")),
               key=os.path.getmtime)
    return c[-1] if c else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?", default=None)
    ap.add_argument("--combined", choices=["row", "col", "both"], default=None,
                    help="render cost+queue as one 3-panel figure")
    ap.add_argument("--cost-csv", default=None, help="cost CSV for --combined")
    ap.add_argument("--queue-csv", default=None, help="queue CSV for --combined")
    ap.add_argument("--queue-metric", choices=["runtime", "wait", "depth"], default="runtime",
                    help="queue panel: total run time (default), queue wait, or depth at submit")
    args = ap.parse_args()

    os.makedirs(IMG, exist_ok=True)

    if args.combined:
        cost_path = args.cost_csv or _newest("cost")
        queue_path = args.queue_csv or _newest("queue")
        if not cost_path or not queue_path:
            sys.exit("Need both a cost and a queue CSV in results/ for --combined.")
        cost_rows, queue_rows = load(cost_path), load(queue_path)
        layouts = ["row", "col"] if args.combined == "both" else [args.combined]
        for lay in layouts:
            out = os.path.join(IMG, f"selection-combined-{lay}.png")
            plot_combined(cost_rows, queue_rows, out, layout=lay,
                          queue_metric=args.queue_metric)
        return

    path = args.csv
    if not path:
        c = sorted(glob.glob(os.path.join(HERE, "results", "selection-*.csv")),
                   key=os.path.getmtime)
        path = c[-1] if c else None
    if not path or not os.path.exists(path):
        sys.exit("No selection-*.csv found in results/. Pass one explicitly.")
    rows = load(path)
    if not rows:
        sys.exit(f"No rows in {path}")
    exp = rows[0].get("experiment", "cost")
    base = os.path.splitext(os.path.basename(path))[0]
    out = os.path.join(IMG, base + ".png")
    if exp == "queue":
        plot_queue(rows, out)
    else:
        plot_cost(rows, out)


if __name__ == "__main__":
    main()
