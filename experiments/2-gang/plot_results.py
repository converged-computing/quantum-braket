#!/usr/bin/env python3
"""
Plot consumer-idle results for experiment 2 (gang scheduling: fluence vs default).

Two modes, auto-detected from the data:

SINGLE BACKEND (rows all share one backend): per-CONFIGURATION view —
  (left)  per-run scatter for each (config x scheduler), every repeat a point;
  (right) mean +/- stdev bars per (config x scheduler).
  Configurations are n_workers x n_nodes; a sweep makes one cluster each.

CROSS BACKEND (rows span >1 backend, or you pass several files): comparison
  across backends (e.g. sv1, tn1, dm1, rigetti, iqm) for a fixed worker count —
  one x-group per backend, default vs fluence bars, so you can show the benefit
  holds across simulators AND real devices (and grows with QPU service time).
  Backends are ordered by mean QPU wait (simulators first, busy QPUs last).

Usage:
  python3 plot_results.py results/combined-tn1-....csv         # single backend
  python3 plot_results.py results/combined-*.csv               # cross-backend
  python3 plot_results.py results/combined-{sv1,tn1,rigetti_cepheus}-*.csv \
        --workers 4 -o cross.png
  python3 plot_results.py            # newest combined-*.csv under results/
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

COLORS = {"default": "#D55E00", "fluence": "#0072B2"}

# Backend classification: simulators have no real queue (controlled lower bound);
# real QPUs do. Names follow the resource graph (sv1/tn1/dm1 are AWS Braket
# simulators; rigetti_*, iqm_* are real devices).
SIMULATORS = {"sv1", "tn1", "dm1"}
def is_simulator(backend):
    b = backend.lower()
    return b in SIMULATORS or "sim" in b or b.endswith("_local")

SCHED_ORDER = ["default", "fluence"]


def load_rows(paths):
    out = []
    seen = set()
    for path in paths:
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                idle_time = r.get("total_consumer_idle_s") or r.get('total_worker_idle_s')
                if not r.get("scheduler") or not idle_time:
                    continue
                try:
                    rec = {
                        "backend":   (r.get("backend") or "?").strip(),
                        "scheduler": r["scheduler"].strip(),
                        "idle":      float(idle_time),
                        "qpu":       float(r.get("qpu_queue_wait_s") or 0),
                        "n_workers": int(float(r.get("n_consumers") or r.get('n_workers') or 0)),  # CSV col n_consumers
                        "n_nodes":   int(float(r.get("n_nodes") or 0)),
                    }
                except ValueError:
                    continue
                # Dedup: the orchestrator writes each run into BOTH a per-arm file
                # (combined-<backend>-<arm>-*.csv) and a combined file
                # (combined-<backend>-*.csv), so globbing all of them reads the
                # same run twice. Key on the run's identity (incl. its timestamp
                # if present) so genuine repeats are kept but exact duplicates are
                # dropped. Without a timestamp column we fall back to the value
                # tuple, which collapses identical-valued rows (safe: averaging a
                # value with its duplicate doesn't change the mean, and it stops
                # inflating the reported run count).
                ts = (r.get("timestamp") or "").strip()
                key = (rec["backend"], rec["scheduler"], rec["n_workers"],
                       rec["n_nodes"], ts,
                       None if ts else (rec["idle"], rec["qpu"]))
                if key in seen:
                    continue
                seen.add(key)
                out.append(rec)
    return out


def expand(paths):
    out = []
    for p in paths:
        if any(c in p for c in "*?["):
            out.extend(sorted(glob.glob(p)))
        else:
            out.append(p)
    return [p for p in out if os.path.exists(p)]


def ms(vals):
    m = st.mean(vals)
    s = st.stdev(vals) if len(vals) > 1 else 0.0
    return m, s


# ── single-backend: per-configuration ────────────────────────────────────────
def plot_single(rows, backend, out, title):
    ws = sorted(set(r["n_workers"] for r in rows))
    ns = sorted(set(r["n_nodes"] for r in rows))
    vary_w, vary_n = len(ws) > 1, len(ns) > 1
    configs = sorted(set((r["n_workers"], r["n_nodes"]) for r in rows))
    scheds = [s for s in SCHED_ORDER if any(r["scheduler"] == s for r in rows)]

    data = {c: {s: [] for s in scheds} for c in configs}
    qpu = {c: {s: [] for s in scheds} for c in configs}
    for r in rows:
        c = (r["n_workers"], r["n_nodes"])
        if r["scheduler"] in data[c]:
            data[c][r["scheduler"]].append(r["idle"])
            qpu[c][r["scheduler"]].append(r["qpu"])

    def label(nw, nn):
        p = []
        if vary_w:
            p.append(f"{nw}w")
        if vary_n:
            p.append(f"{nn}q")
        return " ".join(p) if p else f"{nw}w {nn}q"

    n_cfg = len(configs)
    sub_w = 0.8 / max(len(scheds), 1)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(max(11, 3 * n_cfg + 4), 4.8))
    rng = np.random.default_rng(0)

    for ci, c in enumerate(configs):
        for si, s in enumerate(scheds):
            v = data[c][s]
            if not v:
                continue
            xc = ci + (si - (len(scheds) - 1) / 2) * sub_w
            xs = xc + rng.uniform(-sub_w * 0.3, sub_w * 0.3, size=len(v))
            ax1.scatter(xs, v, color=COLORS[s], s=44, zorder=3,
                        edgecolor="white", linewidth=0.6, label=s if ci == 0 else None)
            ax1.plot([xc - sub_w * 0.35, xc + sub_w * 0.35], [st.median(v)] * 2,
                     color="black", lw=1.6, zorder=4)
    ax1.set_xticks(range(n_cfg)); ax1.set_xticklabels([label(*c) for c in configs])
    ax1.set_ylabel("total consumer idle (s)"); ax1.set_ylim(bottom=0)
    ax1.set_title("Per-run consumer idle  (each point = one repeat; bar = median)")
    ax1.grid(axis="y", alpha=0.3); ax1.legend(title="scheduler")
    if n_cfg > 1:
        ax1.set_xlabel("configuration")

    for ci, c in enumerate(configs):
        for si, s in enumerate(scheds):
            v = data[c][s]
            if not v:
                continue
            xc = ci + (si - (len(scheds) - 1) / 2) * sub_w
            m, sd = ms(v)
            ax2.bar(xc, m, width=sub_w * 0.9, yerr=sd, capsize=5, color=COLORS[s],
                    alpha=0.85, error_kw=dict(ecolor="black", lw=1.2),
                    label=s if ci == 0 else None)
            ax2.text(xc, m + sd, f"{m:.0f}+/-{sd:.0f}", ha="center", va="bottom",
                     fontsize=8.5, fontweight="bold")
    ax2.set_xticks(range(n_cfg)); ax2.set_xticklabels([label(*c) for c in configs])
    ax2.set_ylabel("mean consumer idle (s)"); ax2.set_ylim(bottom=0)
    ax2.set_title("Mean consumer idle  (error bars = stdev)")
    ax2.grid(axis="y", alpha=0.3); ax2.legend(title="scheduler")
    if n_cfg > 1:
        ax2.set_xlabel("configuration")

    alld = [v for c in configs for v in data[c].get("default", [])]
    allf = [v for c in configs for v in data[c].get("fluence", [])]
    nrep = min((len(data[c][s]) for c in configs for s in scheds if data[c][s]), default=1)
    cap = [f"n={nrep} repeat(s)/arm"]
    if alld and allf:
        cap.append(f"median idle reduced {st.median(alld) / st.median(allf):.1f}x with fluence")
    allq = [v for c in configs for s in scheds for v in qpu[c][s]]
    if allq:
        cap.append(f"QPU wait~{st.mean(allq):.1f}s")
    fig.text(0.5, -0.02, " . ".join(cap), ha="center", fontsize=8.5, style="italic")
    fig.suptitle(title or f"Gang scheduling consumer idle - backend={backend}",
                 fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0.03, 1, 0.96])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


# ── cross-backend: one group per backend, fixed worker count ──────────────────
def _cross_panel(ax, rows, fixed_w, scheds, show_legend):
    """Draw one backend-comparison panel for a single worker count onto ax.
    Returns (n_min, n_max, multi_notes) for caption/warnings."""
    rows = [r for r in rows if r["n_workers"] == fixed_w]
    backends = sorted(set(r["backend"] for r in rows))
    data = {b: {s: [] for s in scheds} for b in backends}
    qpu = {b: {s: [] for s in scheds} for b in backends}
    for r in rows:
        if r["scheduler"] in data[r["backend"]]:
            data[r["backend"]][r["scheduler"]].append(r["idle"])
            qpu[r["backend"]][r["scheduler"]].append(r["qpu"])

    notes = []
    for b in backends:
        for s in scheds:
            if len(data[b][s]) > 1:
                notes.append(f"{b}/{s}@{fixed_w}w averages {len(data[b][s])} runs "
                             f"(idle each={[round(x) for x in data[b][s]]})")

    # Stable, deterministic order across ALL panels: simulators first, then real
    # devices, each group alphabetical. NOT sorted by QPU wait — that made the
    # order differ between panels when waits were near-equal.
    backends.sort(key=lambda b: (0 if is_simulator(b) else 1, b))

    sub_w = 0.8 / max(len(scheds), 1)
    for bi, b in enumerate(backends):
        for si, s in enumerate(scheds):
            v = data[b][s]
            if not v:
                continue
            xc = bi + (si - (len(scheds) - 1) / 2) * sub_w
            m, sd = ms(v)
            ax.bar(xc, m, width=sub_w * 0.9, yerr=sd, capsize=5, color=COLORS[s],
                   alpha=0.85, error_kw=dict(ecolor="black", lw=1.2),
                   label=s if (bi == 0 and show_legend) else None)
            ax.text(xc, m + sd, f"{m:.0f}", ha="center", va="bottom",
                    fontsize=8, fontweight="bold")
    # QPU wait per ARM, slashed, in the SAME left-to-right order as the bars
    # (SCHED_ORDER). On simulators the two values match (~2s), confirming both
    # arms saw the same service time; on real QPUs they differ, exposing the
    # uncontrolled-queue confound rather than hiding it behind an average.
    labels = []
    for b in backends:
        per_arm = [f"{st.mean(qpu[b][s]):.1f}" if qpu[b][s] else "-" for s in scheds]
        labels.append(f"{b}\n(QPU {'/'.join(per_arm)}s)")
    ax.set_xticks(range(len(backends)))
    ax.set_xticklabels(labels, fontsize=7.5, rotation=20, ha="right")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", alpha=0.3)
    ax.set_title(f"{fixed_w} workers", fontsize=11, fontweight="bold")
    if show_legend:
        ax.legend(title="scheduler", fontsize=8)
    ns = [len(data[b][s]) for b in backends for s in scheds if data[b][s]]
    return (min(ns) if ns else 0, max(ns) if ns else 0), notes


def plot_cross(rows, out, title, fixed_w):
    scheds = [s for s in SCHED_ORDER if any(r["scheduler"] == s for r in rows)]
    have = sorted(set(r["n_workers"] for r in rows))

    # If a worker count is requested, single panel. Otherwise FACET: one panel per
    # worker count present, so NO data is silently dropped (the previous behavior
    # of collapsing to a single size hid the other sizes' runs).
    wcounts = [fixed_w] if fixed_w is not None else have
    wcounts = [w for w in wcounts if w in have]
    if not wcounts:
        sys.exit(f"no rows with requested worker count; available: {have}")

    n_panels = len(wcounts)
    backends_all = sorted(set(r["backend"] for r in rows))
    n_back = len(backends_all)
    # Width: each panel needs room for its bars; scale by backends-per-panel and
    # panel count so the figure fills horizontal space rather than leaving the
    # panels narrow (e.g. simulators-only with 3 backends shouldn't be squeezed).
    panel_w = max(3.2, 1.1 * n_back + 1.5)
    fig, axes = plt.subplots(
        1, n_panels, figsize=(panel_w * n_panels, 5.2),
        squeeze=False, sharey=False)
    axes = axes[0]

    nmins, nmaxs, all_notes = [], [], []
    for i, w in enumerate(wcounts):
        (nmin, nmax), notes = _cross_panel(axes[i], rows, w, scheds,
                                            show_legend=(i == n_panels - 1))
        nmins.append(nmin); nmaxs.append(nmax); all_notes += notes
        if i == 0:
            axes[i].set_ylabel("mean consumer idle (s)")

    # Report multi-run bars (so averaging across repeats is visible, not hidden).
    for note in all_notes:
        print(f"  note: {note}", file=sys.stderr)

    nmin, nmax = (min(nmins) if nmins else 0), (max(nmaxs) if nmaxs else 0)
    nrep_txt = f"n={nmin}" if nmin == nmax else f"n={nmin}-{nmax}"
    fig.text(0.5, 0.005,
             f"{nrep_txt} run(s)/bar . backends ordered by QPU service/queue time "
             f"(simulators ~2s = controlled lower bound; real-QPU waits are single, "
             f"uncontrolled observations) . benefit grows with QPU time and worker count",
             ha="center", fontsize=8, style="italic")
    fig.suptitle(title or "Worker idle across backends: fluence vs default",
                 fontsize=13, fontweight="bold")
    fig.tight_layout(rect=[0, 0.04, 1, 0.96])
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="*", help="combined-*.csv (default: newest under results/)")
    ap.add_argument("-o", "--out", default=None,
                    help="output path/basename; written under img/ (created if absent)")
    ap.add_argument("--title", default=None)
    ap.add_argument("--workers", type=int, default=None,
                    help="cross-backend mode: worker count to compare (default: max present)")
    ap.add_argument("--cross", action="store_true",
                    help="force cross-backend comparison even for one file")
    ap.add_argument("--img-dir", default="img",
                    help="directory for output images (default: img/, created if absent)")
    args = ap.parse_args()

    paths = expand(args.csv) if args.csv else None
    if not paths:
        c = sorted(glob.glob("results/combined-*.csv"), key=os.path.getmtime)
        paths = [c[-1]] if c else None
    if not paths:
        sys.exit("No CSV found. Pass one: python3 plot_results.py results/combined-....csv")

    rows = load_rows(paths)
    if not rows:
        sys.exit(f"No usable rows in {paths}")

    # All images go under img/ (created if needed). A bare -o name is placed
    # there; an -o with its own directory is honored as-is.
    img_dir = args.img_dir
    os.makedirs(img_dir, exist_ok=True)

    def out_path(name):
        if args.out:
            # if -o has a directory component, respect it; else drop into img_dir
            return args.out if os.path.dirname(args.out) else os.path.join(img_dir, args.out)
        return os.path.join(img_dir, name)

    backends = sorted(set(r["backend"] for r in rows))

    if len(backends) > 1 or args.cross:
        # Three cross-backend figures: all backends, simulators only, real only.
        sims  = sorted(b for b in backends if is_simulator(b))
        reals = sorted(b for b in backends if not is_simulator(b))
        groups = [("all", backends)]
        if sims:
            groups.append(("simulators", sims))
        if reals:
            groups.append(("real", reals))
        # if there's only one category, the "all" plot already covers it; skip dups
        if len(groups) == 2 and groups[1][1] == backends:
            groups = [groups[0]]

        for tag, subset in groups:
            sub_rows = [r for r in rows if r["backend"] in subset]
            if not sub_rows:
                continue
            title = args.title
            if title is None:
                pretty = {"all": "all backends", "simulators": "simulators only",
                          "real": "real QPUs only"}[tag]
                title = f"Worker idle ({pretty}): fluence vs default"
            name = (os.path.splitext(os.path.basename(paths[0]))[0]
                    + f"-cross-{tag}.png")
            plot_cross(sub_rows, out_path(name), title, args.workers)
    else:
        name = os.path.splitext(os.path.basename(paths[0]))[0] + ".png"
        plot_single(rows, backends[0], out_path(name), args.title)

    print("\nstats:")
    for b in backends:
        for s in SCHED_ORDER:
            for nw in sorted(set(r["n_workers"] for r in rows if r["backend"] == b)):
                v = [r["idle"] for r in rows
                     if r["backend"] == b and r["scheduler"] == s and r["n_workers"] == nw]
                if v:
                    print(f"  {b} {nw}w {s:<8} n={len(v)} mean={st.mean(v):.2f} "
                          f"median={st.median(v):.2f} "
                          f"stdev={st.stdev(v) if len(v) > 1 else 0:.2f}")


if __name__ == "__main__":
    main()
