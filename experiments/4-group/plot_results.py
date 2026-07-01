#!/usr/bin/env python3
"""
plot_placement.py — compare the two scheduler arms (fluence vs default-scheduler)
from the contention experiment's results CSV.

One row of three:
  1. pending_s  — submit -> whole gang placed (queue wait)        [boxplot]
  2. total_s    — submit -> application finished (end to end)      [boxplot]
  3. squat      — wasted node-time, TOTAL accumulated per arm      [bar]

Usage:
  python3 plot_placement.py results/contention.csv            # -> contention_placement.png
  python3 plot_placement.py results/contention.csv --out x.png --show
"""
import argparse
import os
import sys

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ARM_LABEL = {"fluence": "Fluence", "default-scheduler": "default-scheduler",
             "default": "default-scheduler"}
# green for Fluence, dark lavender for the default scheduler
COLORS = {"Fluence": "#1f8a76", "default-scheduler": "#7c6ca8"}
EDGES  = {"Fluence": "#13705e", "default-scheduler": "#574a7a"}


def load(path):
    if not os.path.exists(path):
        sys.exit(f"no such file: {path}")
    df = pd.read_csv(path)
    if df.empty:
        sys.exit(f"{path} is empty")
    df["arm"] = df["scheduler"].map(lambda s: ARM_LABEL.get(s, s))
    for c in ["pending_s", "startup_s", "apprun_s", "total_s"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # squat window per gang = scheduled_at (gang fully placed) - app_start_at
    # (first pod's container). ~0 for Fluence (atomic placement); large for the
    # default scheduler. (ready_at would be ~0 for both -- it happens after
    # assembly -- which is why an earlier version's panel was empty.)
    for c in ["scheduled_at", "app_start_at", "ready_at", "app_end_at"]:
        if c in df:
            df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
    if "scheduled_at" in df and "app_start_at" in df:
        df["assembly_skew_s"] = ((df["scheduled_at"] - df["app_start_at"])
                                 .dt.total_seconds().clip(lower=0))
    # "whole gang up" marker: ready_at would be ideal, but the runner records it
    # as NaT (never populated), so fall back to the latest of (gang fully placed,
    # first container start) -- both ARE recorded. run time = that -> app end.
    if "app_end_at" in df and "scheduled_at" in df and "app_start_at" in df:
        gang_up = df[["scheduled_at", "app_start_at"]].max(axis=1)
        df["run_s"] = (df["app_end_at"] - gang_up).dt.total_seconds().clip(lower=0)

    if "squat_node_s" in df:
        df["squat"] = pd.to_numeric(df["squat_node_s"], errors="coerce")
        df["squat_kind"] = "measured"
    elif "assembly_skew_s" in df:
        df["squat"] = df["assembly_skew_s"] * (df["size"] - 1).clip(lower=0)
        df["squat_kind"] = "upper-bound"
    return df


def plot(df, out, show):
    arms = [a for a in ["Fluence", "default-scheduler"] if a in df["arm"].unique()]
    if not arms:
        arms = sorted(df["arm"].unique())

    def style_box(ax, col, title, ylabel):
        if col not in df:
            ax.text(0.5, 0.5, f"no '{col}' column in CSV", ha="center", va="center")
            ax.set_axis_off(); return
        data = [df[df["arm"] == a][col].dropna() for a in arms]
        if not any(len(d) for d in data):
            ax.text(0.5, 0.5, f"no {col} data", ha="center", va="center")
            ax.set_axis_off(); return
        bp = ax.boxplot(data, tick_labels=arms, patch_artist=True, showfliers=True, widths=0.55)
        for patch, a in zip(bp["boxes"], arms):
            patch.set_facecolor(COLORS.get(a, "#90caf9"))
            patch.set_alpha(0.8)
            patch.set_edgecolor(EDGES.get(a, "#444"))
        for med in bp["medians"]:
            med.set_color("#16202a"); med.set_linewidth(1.6)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.25)
        # headroom so the top whisker / fliers are never at the frame edge
        hi = max((d.max() for d in data if len(d)), default=1)
        ax.set_ylim(0, hi * 1.12)

    # wider + extra height so nothing is truncated; constrained_layout handles
    # spacing for the two-line titles without clipping.
    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.6), constrained_layout=True)
    fig.suptitle("Gang scheduling under contention: Fluence vs default-scheduler",
                 fontsize=15, fontweight="bold")

    style_box(axes[0], "pending_s",
              "Time to placement (pending_s)\nsubmit \u2192 gang scheduled", "seconds")
    style_box(axes[1], "total_s",
              "Total time per gang (total_s)\nsubmit \u2192 application finished", "seconds")

    # Panel 3: TOTAL accumulated wasted node-time per arm (bar).
    ax = axes[2]
    if "squat" in df and df["squat"].notna().any():
        kind = df["squat_kind"].iloc[0] if "squat_kind" in df else ""
        totals = [df[df["arm"] == a]["squat"].dropna().sum() for a in arms]
        bars = ax.bar(arms, totals, width=0.55,
                      color=[COLORS.get(a, "#999") for a in arms],
                      edgecolor=[EDGES.get(a, "#444") for a in arms], alpha=0.9)
        hi = max(totals) if any(totals) else 1
        for b, t in zip(bars, totals):
            ax.text(b.get_x() + b.get_width() / 2, t + hi * 0.02, f"{t:.0f}",
                    ha="center", va="bottom", fontsize=11, fontweight="bold")
        ax.set_title(f"Wasted node-time: total accumulated ({kind})\n"
                     "\u03a3 (gang assembled \u2212 first pod start) \u00d7 (size \u2212 1)")
        ax.set_ylabel("node-seconds")
        ax.grid(axis="y", alpha=0.25)
        ax.set_ylim(0, hi * 1.18)   # headroom for the value labels
    else:
        ax.text(0.5, 0.5, "no squat/skew columns in CSV\n"
                "(need scheduled_at + app_start_at, or squat_node_s)",
                ha="center", va="center")
        ax.set_axis_off()

    # bbox_inches='tight' + pad so axis labels/titles are never cut on save
    fig.savefig(out, dpi=150, bbox_inches="tight", pad_inches=0.3)
    print(f"wrote {out}")

    print("\n=== summary ===")
    for a in arms:
        sub = df[df["arm"] == a]
        n = len(sub)
        pend = sub["pending_s"].dropna() if "pending_s" in sub else pd.Series([], dtype=float)
        tot = sub["total_s"].dropna() if "total_s" in sub else pd.Series([], dtype=float)
        squat = sub["squat"].dropna() if "squat" in sub else pd.Series([], dtype=float)
        kind = sub["squat_kind"].iloc[0] if "squat_kind" in sub and len(sub) else ""
        base = f"{a:18s}  gangs={n:3d}"
        if len(pend):
            base += f"  pending med={pend.median():.0f}/max={pend.max():.0f}"
        if len(tot):
            base += f"  total med={tot.median():.0f}"
        if len(squat):
            base += f"  squat({kind}) total={squat.sum():.0f}/max={squat.max():.0f}"
        print(base)
    if show:
        try:
            os.system(f"open {out} 2>/dev/null || xdg-open {out} 2>/dev/null")
        except Exception:
            pass


def plot_by_size(df, out, show):
    """Same three panels, same colors, but broken out by gang size (one grouped
    series per arm). Boxplots for pending_s / total_s; bars for total squat."""
    arms = [a for a in ["Fluence", "default-scheduler"] if a in df["arm"].unique()]
    if not arms:
        arms = sorted(df["arm"].unique())
    sizes = sorted(int(s) for s in df["size"].dropna().unique())
    if not sizes:
        print("no 'size' values; skipping by-size plot"); return

    gw = 0.8                      # width of a size-group
    bw = gw / max(len(arms), 1)   # width per arm within the group

    def positions(ai, n):
        return [i - gw / 2 + bw / 2 + ai * bw for i in range(n)]

    def grouped_box(ax, col, sizes_use, title, ylabel):
        if col not in df:
            ax.text(0.5, 0.5, f"no '{col}'", ha="center", va="center"); ax.set_axis_off(); return
        hi = 1
        for ai, a in enumerate(arms):
            data = [df[(df["arm"] == a) & (df["size"] == s)][col].dropna() for s in sizes_use]
            bp = ax.boxplot(data, positions=positions(ai, len(sizes_use)), widths=bw * 0.86,
                            patch_artist=True, showfliers=True)
            for patch in bp["boxes"]:
                patch.set_facecolor(COLORS.get(a, "#999")); patch.set_alpha(0.8)
                patch.set_edgecolor(EDGES.get(a, "#444"))
            for med in bp["medians"]:
                med.set_color("#16202a"); med.set_linewidth(1.4)
            hi = max([hi] + [d.max() for d in data if len(d)])
        ax.set_xticks(range(len(sizes_use)))
        ax.set_xticklabels([f"n={s}" for s in sizes_use])
        ax.set_xlabel("gang size (pods)")
        ax.set_title(title); ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.25); ax.set_ylim(0, hi * 1.12)

    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.6), constrained_layout=True)
    fig.suptitle("Gang scheduling under contention, by gang size: Fluence vs default-scheduler",
                 fontsize=15, fontweight="bold")

    # Panel 1: worker idle -- the summed member-running-while-others-not-ready
    # node-seconds. The exact per-member sum needs per-pod ready times the runner
    # did not save; this is the upper bound (skew x (size-1)) from the recorded
    # gang-level times, which is what `squat` holds. n=1 has none (size-1=0).
    sizes_sq = [s for s in sizes if s > 1]
    grouped_box(axes[0], "squat", sizes_sq,
                "Worker idle (node-seconds, upper-bound)\n\u03a3 (whole gang up \u2212 member start)", "node-seconds")

    # Panel 2: run time -- whole gang up -> finished (gang fully placed, since
    # ready_at was not recorded). Excludes queue wait and the assembly idle.
    grouped_box(axes[1], "run_s", sizes,
                "Run time (whole gang up \u2192 finished)\napp end \u2212 gang up", "seconds")

    # Panel 3: total accumulated wasted node-time -- excludes n=1 (size-1 = 0).
    ax = axes[2]
    sizes_sq = [s for s in sizes if s > 1]
    if "squat" in df and df["squat"].notna().any() and sizes_sq:
        kind = df["squat_kind"].iloc[0] if "squat_kind" in df else ""
        hi = 1
        for ai, a in enumerate(arms):
            totals = [df[(df["arm"] == a) & (df["size"] == s)]["squat"].dropna().sum() for s in sizes_sq]
            ax.bar(positions(ai, len(sizes_sq)), totals, width=bw * 0.86,
                   color=COLORS.get(a, "#999"), edgecolor=EDGES.get(a, "#444"), alpha=0.9)
            hi = max([hi] + totals)
            for x, t in zip(positions(ai, len(sizes_sq)), totals):
                if t > 0:
                    ax.text(x, t + hi * 0.02, f"{t:.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
        ax.set_xticks(range(len(sizes_sq)))
        ax.set_xticklabels([f"n={s}" for s in sizes_sq])
        ax.set_xlabel("gang size (pods)")
        ax.set_title(f"Wasted node-time: total accumulated ({kind})\n"
                     "\u03a3 (gang assembled \u2212 first pod start) \u00d7 (size \u2212 1)")
        ax.set_ylabel("node-seconds")
        ax.grid(axis="y", alpha=0.25); ax.set_ylim(0, hi * 1.18)
    else:
        ax.text(0.5, 0.5, "no squat data (size > 1)", ha="center", va="center"); ax.set_axis_off()

    # one shared legend for the two arms
    handles = [plt.Rectangle((0, 0), 1, 1, facecolor=COLORS.get(a, "#999"),
                             edgecolor=EDGES.get(a, "#444"), alpha=0.85) for a in arms]
    fig.legend(handles, arms, loc="upper right", ncol=len(arms), frameon=False, fontsize=11)

    fig.savefig(out, dpi=150, bbox_inches="tight", pad_inches=0.3)
    print(f"wrote {out}")
    if show:
        try:
            os.system(f"open {out} 2>/dev/null || xdg-open {out} 2>/dev/null")
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?", default="results/contention.csv")
    ap.add_argument("--out", default=None)
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args()
    out = args.out or os.path.splitext(os.path.basename(args.csv))[0] + "_placement.png"
    df = load(args.csv)
    plot(df, out, args.show)
    by_size_out = os.path.splitext(out)[0] + "_bysize.png"
    plot_by_size(df, by_size_out, args.show)
    by_size_out = os.path.splitext(out)[0] + "_bysize.svg"
    plot_by_size(df, by_size_out, args.show)


if __name__ == "__main__":
    main()
