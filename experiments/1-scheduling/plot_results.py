#!/usr/bin/env python3
"""
plot_results.py — Plot two-queue characterization experiment results.

Reads combined-*.csv files from the results/ directory and produces
figures comparing backends across key metrics.

Hue = backend (quantum device/vendor).

Usage:
  python3 plot_results.py                        # reads results/combined-*.csv
  python3 plot_results.py --results path/to/dir
  python3 plot_results.py --out figures/
"""

import argparse
import glob
import os
import sys

import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

matplotlib.rcParams.update({
    "font.family":      "sans-serif",
    "font.size":        11,
    "axes.titlesize":   12,
    "axes.labelsize":   11,
    "legend.fontsize":  10,
    "figure.dpi":       150,
})

BACKEND_PALETTE = {
    "sv1":              "#2196F3",
    "tn1":              "#00BCD4",
    "dm1":              "#4CAF50",
    "ahs_local":        "#9E9E9E",
    "aquila":           "#E91E63",
    "rigetti_cepheus":  "#FF5722",
    "iqm_garnet":       "#9C27B0",
    "iqm_emerald":      "#673AB7",
    "aqt_ibex":         "#795548",
    "ionq_forte":       "#F44336",
}

BACKEND_LABELS = {
    "sv1":              "Amazon SV1",
    "tn1":              "Amazon TN1",
    "dm1":              "Amazon DM1",
    "ahs_local":        "Local AHS sim",
    "aquila":           "QuEra Aquila",
    "rigetti_cepheus":  "Rigetti Cepheus",
    "iqm_garnet":       "IQM Garnet",
    "iqm_emerald":      "IQM Emerald",
    "aqt_ibex":         "AQT IBEX-Q1",
    "ionq_forte":       "IonQ Forte",
}


def load_results(results_dir):
    pattern = os.path.join(results_dir, "combined-*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"ERROR: no combined-*.csv files found in {results_dir}", file=sys.stderr)
        print(f"Files present: {os.listdir(results_dir)}", file=sys.stderr)
        sys.exit(1)

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            frames.append(df)
            print(f"  loaded {f}  ({len(df)} rows)")
        except Exception as e:
            print(f"  WARNING: could not read {f}: {e}", file=sys.stderr)

    df = pd.concat(frames, ignore_index=True)

    numeric = [
        "n_pods_batch", "n_nodes_or_atoms", "n_shots", "max_iter",
        "pod_wall_s", "batch_wall_s", "n_iterations",
        "total_qpu_wait_s", "total_circuit_time_s",
        "avg_qpu_wait_s", "avg_circuit_time_s",
        "approximation_ratio", "total_elapsed_s",
    ]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Force integer types for categorical axes
    df["n_pods_batch"]      = df["n_pods_batch"].astype("Int64")
    df["n_nodes_or_atoms"]  = df["n_nodes_or_atoms"].astype("Int64")

    df["backend_label"] = df["backend"].map(
        lambda b: BACKEND_LABELS.get(b, b)
    )

    # Classical idle fraction — only where we have timing data
    has_timing = df["total_circuit_time_s"].notna() & (df["total_circuit_time_s"] > 0) \
               & df["pod_wall_s"].notna() & (df["pod_wall_s"] > 0)
    df["idle_fraction"] = None
    df.loc[has_timing, "idle_fraction"] = (
        df.loc[has_timing, "total_circuit_time_s"]
        / df.loc[has_timing, "pod_wall_s"]
    )
    df["idle_fraction"] = pd.to_numeric(df["idle_fraction"], errors="coerce")

    print(f"\n  total rows      : {len(df)}")
    print(f"  backends        : {sorted(df['backend'].unique())}")
    print(f"  problem sizes   : {sorted(df['n_nodes_or_atoms'].dropna().unique())}")
    print(f"  concurrency     : {sorted(df['n_pods_batch'].dropna().unique())}")
    print(f"  has timing data : {has_timing.sum()} rows")
    print(f"  has approx ratio: {df['approximation_ratio'].notna().sum()} rows")
    return df


def backend_palette(df):
    return {
        BACKEND_LABELS.get(b, b): BACKEND_PALETTE.get(b, "#607D8B")
        for b in df["backend"].unique()
    }


def add_panel_label(ax, label):
    ax.text(-0.12, 1.02, label, transform=ax.transAxes,
            fontweight="bold", fontsize=13, va="bottom")


def note_no_data(ax, msg="No timing data collected"):
    ax.text(0.5, 0.5, msg, transform=ax.transAxes,
            ha="center", va="center", fontsize=10, color="grey",
            style="italic")


def x_as_int(ax, df, col):
    """Set x ticks to the actual integer values present in df[col]."""
    vals = sorted(df[col].dropna().unique())
    ax.set_xticks([int(v) for v in vals])


# ── Individual panel functions ─────────────────────────────────────────────────

def plot_pod_wall_vs_size(df, ax, palette):
    data = df.dropna(subset=["pod_wall_s", "n_nodes_or_atoms"])
    if data.empty:
        note_no_data(ax, "No pod_wall_s data"); return
    sns.boxplot(
        data=data,
        x="n_nodes_or_atoms", y="pod_wall_s",
        hue="backend_label", palette=palette,
        ax=ax, linewidth=0.8, fliersize=3,
    )
    ax.set_xlabel("Problem size (qubits / atoms)")
    ax.set_ylabel("Pod wall time (s)")
    ax.set_title("Pod wall time vs. problem size")
    ax.legend(title="Backend", loc="upper left")


def plot_pod_wall_vs_concurrency(df, ax, palette):
    data = df.dropna(subset=["pod_wall_s", "n_pods_batch"])
    if data.empty:
        note_no_data(ax, "No pod_wall_s data"); return
    sns.boxplot(
        data=data,
        x="n_pods_batch", y="pod_wall_s",
        hue="backend_label", palette=palette,
        ax=ax, linewidth=0.8, fliersize=3,
    )
    ax.set_xlabel("Concurrent pods (N)")
    ax.set_ylabel("Pod wall time (s)")
    ax.set_title("Pod wall time vs. concurrency")
    ax.legend(title="Backend", loc="upper left")


def plot_batch_wall_vs_concurrency(df, ax, palette):
    data = df.dropna(subset=["batch_wall_s", "n_pods_batch"])
    if data.empty:
        note_no_data(ax, "No batch_wall_s data"); return
    agg = data.groupby(["backend_label", "n_pods_batch"],
                       as_index=False)["batch_wall_s"].mean()
    sns.lineplot(
        data=agg, x="n_pods_batch", y="batch_wall_s",
        hue="backend_label", palette=palette,
        marker="^", ax=ax,
    )
    x_as_int(ax, data, "n_pods_batch")
    ax.set_xlabel("Concurrent pods (N)")
    ax.set_ylabel("Batch wall time (s)")
    ax.set_title("Batch wall time vs. concurrency")
    ax.legend(title="Backend", loc="upper left")


def plot_total_elapsed_vs_size(df, ax, palette):
    data = df.dropna(subset=["total_elapsed_s", "n_nodes_or_atoms"])
    if data.empty:
        note_no_data(ax, "No total_elapsed_s data"); return
    sns.boxplot(
        data=data,
        x="n_nodes_or_atoms", y="total_elapsed_s",
        hue="backend_label", palette=palette,
        ax=ax, linewidth=0.8, fliersize=3,
    )
    ax.set_xlabel("Problem size (qubits / atoms)")
    ax.set_ylabel("Total optimizer elapsed (s)")
    ax.set_title("Optimizer elapsed time vs. problem size")
    ax.legend(title="Backend", loc="upper left")


def plot_qpu_wait_vs_concurrency(df, ax, palette):
    data = df.dropna(subset=["avg_qpu_wait_s", "n_pods_batch"])
    data = data[data["avg_qpu_wait_s"] > 0]
    if data.empty:
        note_no_data(ax, "No QPU wait timing data\n(TIMING log lines not captured)"); return
    sns.lineplot(
        data=data, x="n_pods_batch", y="avg_qpu_wait_s",
        hue="backend_label", palette=palette,
        marker="o", ax=ax, errorbar="sd",
    )
    x_as_int(ax, data, "n_pods_batch")
    ax.set_xlabel("Concurrent pods (N)")
    ax.set_ylabel("Avg QPU queue wait per iter (s)")
    ax.set_title("QPU queue wait vs. concurrency")
    ax.legend(title="Backend", loc="upper left")


def plot_circuit_time_vs_size(df, ax, palette):
    data = df.dropna(subset=["avg_circuit_time_s", "n_nodes_or_atoms"])
    data = data[data["avg_circuit_time_s"] > 0]
    if data.empty:
        note_no_data(ax, "No circuit timing data\n(TIMING log lines not captured)"); return
    sns.lineplot(
        data=data, x="n_nodes_or_atoms", y="avg_circuit_time_s",
        hue="backend_label", palette=palette,
        marker="s", ax=ax, errorbar="sd",
    )
    ax.set_xlabel("Problem size (qubits / atoms)")
    ax.set_ylabel("Avg circuit time per iter (s)")
    ax.set_title("Circuit time (submit→result) vs. problem size")
    ax.legend(title="Backend", loc="upper left")


def plot_classical_idle_fraction(df, ax, palette):
    data = df.dropna(subset=["idle_fraction", "n_pods_batch"])
    data = data[data["idle_fraction"] > 0]
    if data.empty:
        note_no_data(ax, "No idle fraction data\n(requires timing data)"); return
    sns.boxplot(
        data=data, x="n_pods_batch", y="idle_fraction",
        hue="backend_label", palette=palette,
        ax=ax, linewidth=0.8, fliersize=3,
    )
    ax.set_xlabel("Concurrent pods (N)")
    ax.set_ylabel("QPU wait / pod wall time")
    ax.set_title("Classical idle fraction vs. concurrency")
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1))
    ax.legend(title="Backend", loc="upper left")


def plot_approximation_ratio(df, ax, palette):
    data = df.dropna(subset=["approximation_ratio"])
    data = data[data["approximation_ratio"] > 0]
    if data.empty:
        note_no_data(ax, "No approximation_ratio data"); return
    order = sorted(data["backend_label"].unique())
    pal   = {k: v for k, v in palette.items() if k in order}
    sns.boxplot(
        data=data, x="backend_label", y="approximation_ratio",
        order=order, palette=pal,
        ax=ax, linewidth=0.8, fliersize=3,
    )
    ax.set_xlabel("Backend")
    ax.set_ylabel("Approximation ratio")
    ax.set_title("QAOA solution quality by backend")
    ax.tick_params(axis="x", rotation=20)
    ax.axhline(0.6924, color="grey", linestyle="--", linewidth=0.8,
               label="p=1 bound (3-reg)")
    ax.legend(fontsize=9)


def plot_iterations_vs_size(df, ax, palette):
    data = df.dropna(subset=["n_iterations", "n_nodes_or_atoms"])
    data = data[data["n_iterations"] > 0]
    if data.empty:
        note_no_data(ax, "No n_iterations data"); return
    sns.boxplot(
        data=data, x="n_nodes_or_atoms", y="n_iterations",
        hue="backend_label", palette=palette,
        ax=ax, linewidth=0.8, fliersize=3,
    )
    ax.set_xlabel("Problem size (qubits / atoms)")
    ax.set_ylabel("COBYLA iterations")
    ax.set_title("Iterations to convergence vs. problem size")
    ax.legend(title="Backend", loc="upper left")


def plot_approx_ratio_vs_size(df, ax, palette):
    data = df.dropna(subset=["approximation_ratio", "n_nodes_or_atoms"])
    data = data[data["approximation_ratio"] > 0]
    if data.empty:
        note_no_data(ax, "No approximation_ratio data"); return
    sns.lineplot(
        data=data, x="n_nodes_or_atoms", y="approximation_ratio",
        hue="backend_label", palette=palette,
        marker="o", ax=ax, errorbar="sd",
    )
    ax.axhline(0.6924, color="grey", linestyle="--", linewidth=0.8,
               label="p=1 bound (3-reg)")
    ax.set_xlabel("Problem size (qubits / atoms)")
    ax.set_ylabel("Approximation ratio")
    ax.set_title("Solution quality vs. problem size")
    ax.legend(title="Backend", loc="lower left")


# ── Figure assembly ────────────────────────────────────────────────────────────

def make_figure(df, out_dir):
    palette = backend_palette(df)
    os.makedirs(out_dir, exist_ok=True)

    # ── Figure 1: Wall time and concurrency ───────────────────────────────────
    fig1, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig1.suptitle("Two-queue characterization: wall time", fontsize=14, y=1.01)

    plot_pod_wall_vs_size(df, axes[0, 0], palette)
    add_panel_label(axes[0, 0], "A")

    plot_pod_wall_vs_concurrency(df, axes[0, 1], palette)
    add_panel_label(axes[0, 1], "B")

    plot_batch_wall_vs_concurrency(df, axes[1, 0], palette)
    add_panel_label(axes[1, 0], "C")

    plot_total_elapsed_vs_size(df, axes[1, 1], palette)
    add_panel_label(axes[1, 1], "D")

    fig1.tight_layout()
    save(fig1, out_dir, "fig1_wall_time")

    # ── Figure 2: QPU timing (needs TIMING log data) ──────────────────────────
    fig2, axes2 = plt.subplots(1, 3, figsize=(18, 5))
    fig2.suptitle("Two-queue characterization: QPU timing", fontsize=14)

    plot_qpu_wait_vs_concurrency(df, axes2[0], palette)
    add_panel_label(axes2[0], "A")

    plot_circuit_time_vs_size(df, axes2[1], palette)
    add_panel_label(axes2[1], "B")

    plot_classical_idle_fraction(df, axes2[2], palette)
    add_panel_label(axes2[2], "C")

    fig2.tight_layout()
    save(fig2, out_dir, "fig2_qpu_timing")

    # ── Figure 3: Solution quality ────────────────────────────────────────────
    fig3, axes3 = plt.subplots(1, 3, figsize=(18, 5))
    fig3.suptitle("Two-queue characterization: solution quality", fontsize=14)

    plot_approximation_ratio(df, axes3[0], palette)
    add_panel_label(axes3[0], "A")

    plot_approx_ratio_vs_size(df, axes3[1], palette)
    add_panel_label(axes3[1], "B")

    plot_iterations_vs_size(df, axes3[2], palette)
    add_panel_label(axes3[2], "C")

    fig3.tight_layout()
    save(fig3, out_dir, "fig3_quality")

    plt.close("all")


def save(fig, out_dir, name):
    for ext in ("pdf", "png"):
        p = os.path.join(out_dir, f"{name}.{ext}")
        fig.savefig(p, bbox_inches="tight")
    print(f"  saved {name}.pdf / .png")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Plot two-queue characterization results"
    )
    parser.add_argument("--results", default="results/",
                        help="Directory containing combined-*.csv files (default: results/)")
    parser.add_argument("--out", default="figures/",
                        help="Output directory for figures (default: figures/)")
    args = parser.parse_args()

    print(f"Loading combined CSVs from: {args.results}")
    df = load_results(args.results)

    print(f"\nGenerating figures -> {args.out}")
    make_figure(df, args.out)
    print("\nDone.")


if __name__ == "__main__":
    main()
