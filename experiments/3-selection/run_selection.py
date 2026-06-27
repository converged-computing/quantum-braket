#!/usr/bin/env python3
"""
run_selection.py — Cost/queue-aware backend selection experiment (Experiment 5).

Compares a capability-only request (cost/queue-blind) against the same request
plus a `kubectl fluence` selection policy (min-cost or min-queue). The plugin
resolves the backend client-side and pins it; Fluence honors the pin.

Two sub-experiments:

  A (cost):  candidate pool = simulators + Rigetti. Baseline lets Fluence match
             by capability alone (may land on an expensive QPU); min-cost pins
             the cheapest satisfying backend. Metric: realized USD per run.

  B (queue): candidate pool = real QPUs only (simulators have no queue).
             Baseline matches some QPU; min-queue pins the shortest-queue online
             device (live, via the plugin's braket-live provider). Metric:
             realized queue wait. QPU-only => costs real money; keep shots tiny.

Per run we record:
    arm                baseline | min-cost | min-queue
    policy             the select-policy string (empty for baseline)
    realized_backend   the backend the sampler used (from its TIMING log
                       or the stamped annotation)
    realized_cost_usd  computed from the attribute file + shot count
    queue_at_submit    queue depth of the chosen device at submit (sub-exp B)
    leader_wall_s, qpu_queue_wait_s   timing (same derivation as Experiment 2)

Usage:
  # cost sub-experiment, 10 repeats per arm (cheap; baseline may hit Rigetti)
  python3 run_selection.py --experiment cost --repeat 10

  # queue sub-experiment, QPU-only, few repeats (REAL MONEY; 100 shots default)
  python3 run_selection.py --experiment queue --repeat 3

  # dry-run: print what would be submitted, do not apply
  python3 run_selection.py --experiment cost --repeat 2 --dry-run

  # shots default to 100; raise only if you specifically want it (no effect on
  # selection, just higher cost): --n-shots 1000

PREREQUISITES (see README.md):
  - kubectl context pointing at a Fluence-enabled cluster (K8s 1.36.x)
  - `kubectl fluence` plugin on PATH (github.com/converged-computing/kubectl-fluence)
  - fluence-resources ConfigMap listing the candidate backend names
  - cost-attributes.yaml present (this dir)
  - AWS creds configured (only needed for queue sub-experiment / real QPUs)
"""
import argparse
import csv
import datetime
import json
import os
import subprocess
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
ATTR_FILE = os.path.join(HERE, "cost-attributes.yaml")
TEMPLATE = os.path.join(HERE, "manifests", "gang-template.yaml")
RESULTS_DIR = os.path.join(HERE, "results")
NAMESPACE = os.environ.get("FLUENCE_NS", "default")

# Candidate pools per sub-experiment.
POOL_COST = ["sv1", "dm1", "tn1", "rigetti_cepheus", "iqm_garnet", "iqm_emerald"]
POOL_QUEUE = ["rigetti_cepheus", "iqm_garnet", "iqm_emerald"]  # real QPUs only

# Selection annotation block (indented to sit under metadata.annotations).
SELECT_BLOCK = (
    '    fluence.flux-framework.org/select-backend: "braket"\n'
    '    fluence.flux-framework.org/select-policy: "{policy}"\n'
    '    fluence.flux-framework.org/select-candidates: "{candidates}"\n'
    '    fluence.flux-framework.org/select-shots: "{shots}"'
)


# ----------------------------------------------------------------------------
# attribute-file cost model (MUST match the plugin's formula so accounting and
# selection agree).
# ----------------------------------------------------------------------------
def load_attrs():
    import yaml  # PyYAML; pip install pyyaml
    with open(ATTR_FILE) as f:
        doc = yaml.safe_load(f)
    out = {}
    for b in doc.get("backends", []):
        out[b["name"]] = b
    return out


def cost_for(attrs, backend, shots):
    """Per-request USD for `backend` at `shots`, from the attribute file."""
    b = attrs.get(backend)
    if not b:
        return None
    if "cost_per_task" in b or "cost_per_shot" in b:
        return float(b.get("cost_per_task", 0.0)) + shots * float(b.get("cost_per_shot", 0.0))
    if "cost_per_minute" in b:
        # simulators: nominal small cost so they rank below QPUs (same convention
        # the plugin uses).
        return float(b["cost_per_minute"]) * 0.05
    return 0.0


# ----------------------------------------------------------------------------
# live queue snapshot (sub-experiment B) — uses AWS CLI, user creds.
# ----------------------------------------------------------------------------
def queue_depth(attrs, backend):
    """Return the device's normal-queue depth now, or None if unavailable."""
    b = attrs.get(backend)
    if not b or "device_arn" not in b:
        return None
    try:
        out = subprocess.check_output(
            ["aws", "braket", "get-device",
             "--device-arn", b["device_arn"],
             "--region", b.get("region", "us-east-1"),
             "--output", "json"],
            stderr=subprocess.DEVNULL)
        info = json.loads(out)
        for q in info.get("deviceQueueInfo", []):
            if "QUANTUM_TASKS" in q.get("queue", "").upper() and \
               q.get("queuePriority", "Normal") != "Priority":
                qs = q.get("queueSize", "0")
                return 4001 if qs.startswith(">") else int(qs)
    except Exception as e:
        print(f"  queue_depth({backend}) failed: {e}", file=sys.stderr)
    return None


# ----------------------------------------------------------------------------
# manifest rendering + submission
# ----------------------------------------------------------------------------
def render(run_name, run_id, n_shots, group_size, select_block):
    """Render the hybrid quantum gang as a SINGLE pod spec cloned to one leader
    + (group_size-1) workers. Every pod is identical except the
    fluence.flux-framework.org/role annotation (leader|worker); the application
    image is the same and discovers its role at runtime from $FLUENCE_ROLE, which
    Fluence injects from that annotation. Fluence creates the PodGroup from the
    shared group label, gives the leader the sidecar, and gates the workers.

    The leader carries the select-* block (selection arm); workers do not — the
    backend is selected once for the group (the leader submits the quantum task).
    """
    import yaml
    with open(TEMPLATE) as f:
        tmpl = f.read()

    def one(pod_name, role, with_select):
        m = tmpl
        # leader (selection arm) gets the select block appended to its annotations;
        # workers and the baseline leader get the token removed.
        if with_select and select_block.strip():
            m = m.replace("__SELECT_BLOCK__", select_block.rstrip("\n"))
        else:
            m = m.replace("\n__SELECT_BLOCK__", "")
        repl = {
            "__RUN_NAME__": run_name,
            "__POD_NAME__": pod_name,
            "__ROLE__": role,
            "__RUN_ID__": run_id,
            "__N_SHOTS__": str(n_shots),
            "__N_WORKERS__": str(group_size - 1),
            "__SCHEDULER__": "fluence",
        }
        for k, v in repl.items():
            m = m.replace(k, v)
        docs = [d for d in yaml.safe_load_all(m) if d]
        return docs[0]  # the single pod

    pods = [one(f"{run_name}-leader", "leader", with_select=True)]
    for i in range(group_size - 1):
        pods.append(one(f"{run_name}-worker-{i}", "worker", with_select=False))
    # The leader carries expected-workers so Fluence's sidecar waits for all
    # workers before ungating. (Leader-only; workers don't need it.)
    pods[0]["metadata"]["annotations"]["fluence.flux-framework.org/expected-workers"] = str(group_size - 1)
    return yaml.dump_all(pods, sort_keys=False)


def submit(manifest, arm, dry_run):
    """Submit the workload (independent sampler pods).

    baseline arm:   plain `kubectl apply` (no selection — Fluence matches by
                    capability alone).
    selection arm:  a SINGLE `kubectl fluence apply`, which is the real user
                    workflow: the plugin resolves the backend, stamps the
                    annotation, and applies in one shot. We read the chosen
                    backend from the plugin's stderr ("selected backend "X" ...")
                    rather than pre-calling select.

    For dry-run we use `kubectl fluence select` (the resolve-and-print verb) so
    nothing is applied but the resolution is visible.

    Returns (ok, stamped_backend_or_None, stderr_text).
    """
    if arm == "baseline":
        if dry_run:
            print(f"--- DRY RUN ({arm}) ---\n{manifest}\n--- end ---")
            return True, None, ""
        r = subprocess.run(["kubectl", "apply", "-n", NAMESPACE, "-f", "-"],
                           input=manifest, text=True, capture_output=True)
        return (r.returncode == 0), None, r.stderr

    # selection arm
    if dry_run:
        sel = subprocess.run(
            ["kubectl", "fluence", "select", "-n", NAMESPACE,
             "--attributes", ATTR_FILE, "-f", "-"],
            input=manifest, text=True, capture_output=True)
        stamped = _parse_stamped_backend(sel.stdout) or _backend_from_stderr(sel.stderr)
        print(f"--- DRY RUN ({arm}) ---")
        if sel.stderr.strip():
            print(sel.stderr.rstrip())
        print(f"  stamped backend: {stamped or '(none — check plugin/ConfigMap/names)'}")
        print("--- end ---")
        return True, stamped, sel.stderr

    # the real workflow: one command resolves + stamps + applies.
    r = subprocess.run(
        ["kubectl", "fluence", "apply", "-n", NAMESPACE,
         "--attributes", ATTR_FILE, "-f", "-"],
        input=manifest, text=True, capture_output=True)
    stamped = _backend_from_stderr(r.stderr)
    return (r.returncode == 0), stamped, r.stderr


def _backend_from_stderr(stderr):
    """Parse `selected backend "X" for ...` from the plugin's stderr."""
    for line in stderr.splitlines():
        s = line.strip()
        if s.startswith("selected backend "):
            # selected backend "sv1" for Pod/g-leader (policy "min-cost")
            try:
                return s.split('"', 2)[1]
            except IndexError:
                pass
    return None


def _parse_stamped_backend(select_stdout):
    """Pull the pinned backend out of `kubectl fluence select` stdout (the
    rendered manifest's backend annotation)."""
    for line in select_stdout.splitlines():
        s = line.strip()
        if s.startswith("fluence.flux-framework.org/backend:"):
            return s.split(":", 1)[1].strip()
    return None


# ----------------------------------------------------------------------------
# realized backend + timing readback (from the leader pod's TIMING logs)
# ----------------------------------------------------------------------------
def wait_and_collect(run_name, group_size, attrs=None, poll_s=15, heartbeat_s=120):
    """Wait for the gang to finish — for as long as it takes. A quantum task can
    sit in the vendor queue for 30-60+ minutes, so there is NO timeout: we poll
    the leader pod until it reaches a terminal phase (Succeeded/Failed). A
    heartbeat line is logged periodically so a long, healthy queue wait is
    visibly alive rather than looking hung. Returns realized backend + timings
    parsed from the leader's logs.

    To interrupt a run, use Ctrl-C (KeyboardInterrupt) — that is the deliberate
    stop, not an automatic deadline that could kill a run that is merely slow.
    """
    pod0 = f"{run_name}-leader"
    phase = None
    start = time.time()
    next_heartbeat = start + heartbeat_s
    while True:
        phase = _pod_phase(pod0)
        if phase in ("Succeeded", "Failed"):
            break
        now = time.time()
        if now >= next_heartbeat:
            waited = int(now - start)
            print(f"    [{run_name}] still waiting ({waited // 60}m{waited % 60}s) "
                  f"— leader phase={phase} (quantum queue can take 30-60+ min)")
            next_heartbeat = now + heartbeat_s
        time.sleep(poll_s)
    logs = _pod_logs(pod0)
    rec = _parse_timing(logs, attrs)
    rec["leader_phase"] = phase  # kept as the CSV column name
    return rec


def _pod_phase(pod):
    try:
        out = subprocess.check_output(
            ["kubectl", "get", "pod", pod, "-n", NAMESPACE,
             "-o", "jsonpath={.status.phase}"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except Exception:
        return None


def _pod_logs(pod):
    try:
        return subprocess.check_output(
            ["kubectl", "logs", pod, "-n", NAMESPACE],
            stderr=subprocess.DEVNULL).decode()
    except Exception:
        return ""


def _parse_timing(logs, attrs=None):
    """Extract realized backend + timestamps from the leader's actual log format.

    gang.py emits timestamps as:   TIMING <key>_ts=<epoch>
      e.g.  TIMING leader_start_ts=...  TIMING queued_ts=...  TIMING result_ts=...
    and logs the chosen device on a line like:
            [leader] device=<arn> n_shots=... n_workers=...
    The backend NAME is the device's name in the attribute file (ARN -> name);
    if FLUXION_BACKEND was logged we prefer that.
    """
    rec = {"realized_backend": None, "t_queued": None, "t_result": None,
           "t_leader_start": None, "t_leader_end": None, "t_submit": None}
    # map ARN -> backend name from the attribute file (for the device= line)
    arn_to_name = {}
    if attrs:
        for name, b in attrs.items():
            if b.get("device_arn"):
                arn_to_name[b["device_arn"]] = name

    # the *_ts= keys we care about, mapped to our record keys
    ts_map = {
        "leader_start_ts": "t_leader_start",
        "submit_ts": "t_submit",
        "queued_ts": "t_queued",
        "result_ts": "t_result",
        "workers_done_ts": "t_leader_end",  # closest "leader finished useful work"
    }
    for line in logs.splitlines():
        # timestamps: TIMING <key>_ts=<value>
        if "TIMING" in line:
            for tok in line.split():
                if "_ts=" in tok:
                    k, _, v = tok.partition("=")
                    if k in ts_map:
                        try:
                            rec[ts_map[k]] = float(v)
                        except ValueError:
                            pass
        # backend: prefer an explicit FLUXION_BACKEND=, else the device= ARN line
        if "FLUXION_BACKEND=" in line:
            for tok in line.split():
                if tok.startswith("FLUXION_BACKEND="):
                    rec["realized_backend"] = tok.split("=", 1)[1].strip()
        if rec["realized_backend"] is None and "device=" in line:
            for tok in line.split():
                if tok.startswith("device="):
                    dev = tok.split("=", 1)[1].strip()
                    rec["realized_backend"] = arn_to_name.get(dev, dev)
    return rec


def derive(rec):
    """Return (qpu_queue_wait_s, leader_wall_s, backend_latency_s).
    - qpu_queue_wait_s: queued -> result (time the task spent in the vendor queue + run)
    - leader_wall_s:    leader_start -> workers_done (whole leader lifetime)
    - backend_latency_s: submit -> result (full backend turnaround: enqueue + queue + run),
      the "LONG time" you observe waiting on the device.
    """
    qw = None
    if rec.get("t_result") and rec.get("t_queued"):
        qw = rec["t_result"] - rec["t_queued"]
    lw = None
    if rec.get("t_leader_end") and rec.get("t_leader_start"):
        lw = rec["t_leader_end"] - rec["t_leader_start"]
    bl = None
    if rec.get("t_result") and rec.get("t_submit"):
        bl = rec["t_result"] - rec["t_submit"]
    return qw, lw, bl


# ----------------------------------------------------------------------------
# cleanup
# ----------------------------------------------------------------------------
def cleanup(run_name, group_size):
    pods = [f"{run_name}-leader"] + [f"{run_name}-worker-{i}" for i in range(group_size - 1)]
    subprocess.run(["kubectl", "delete", "pod", "-n", NAMESPACE,
                    "--ignore-not-found", *pods],
                   capture_output=True)
    # No PodGroup is created in this independent-pods design, so nothing else to
    # reap (standalone quantum pods carry no group label).


# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--experiment", choices=["cost", "queue"], required=True)
    ap.add_argument("--repeat", type=int, default=5)
    ap.add_argument("--n-shots", type=int, default=100,
                    help="shot count. Default 100. Shot count scales every "
                         "backend's cost identically, so it does NOT change the "
                         "selection ranking or which backend min-cost picks; it "
                         "only scales the price. This experiment measures "
                         "selection, not quantum-result fidelity, so 100 is "
                         "plenty and ~10x cheaper than 1000 on real-QPU runs.")
    ap.add_argument("--group-size", type=int, default=2,
                    help="number of independent QPU-requesting sampler pods per "
                         "run (default 2). Each selects a backend and runs "
                         "independently; we are measuring selection/cost, not "
                         "gang coordination.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    attrs = load_attrs()
    if args.experiment == "cost":
        # No qubit floor: capability matching (qubit count, etc.) is the
        # scheduler's job against the resource graph. The candidate list scopes
        # the pool; the policy is simply min-cost.
        pool, policy = POOL_COST, "min-cost"
        arms = ["baseline", "min-cost"]
    else:
        pool, policy = POOL_QUEUE, "online-only,min-queue"
        arms = ["baseline", "min-queue"]

    candidates = ",".join(pool)
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
    out_csv = os.path.join(RESULTS_DIR, f"selection-{args.experiment}-{ts}.csv")
    os.makedirs(RESULTS_DIR, exist_ok=True)

    fields = ["experiment", "arm", "policy", "repeat", "n_shots", "group_size",
              "realized_backend", "stamped_backend",
              "realized_cost_usd", "queue_at_submit", "qpu_queue_wait_s",
              "backend_latency_s", "leader_wall_s", "leader_phase", "timestamp"]
    rows = []

    print(f"Experiment {args.experiment}: arms={arms} pool={pool} repeats={args.repeat}")
    print(f"  policy(selection arm) = {policy}")
    print(f"  writing -> {out_csv}\n")

    for rep in range(args.repeat):
        for arm in arms:
            run_name = f"sel-{args.experiment}-{arm}-{rep}-{uuid.uuid4().hex[:6]}"
            run_id = f"{run_name}-{ts}"

            if arm == "baseline":
                select_block = ""
            else:
                select_block = SELECT_BLOCK.format(
                    policy=policy, candidates=candidates, shots=args.n_shots)

            manifest = render(run_name, run_id, args.n_shots, args.group_size,
                              select_block)

            # snapshot candidate queues at submit (sub-exp B, and informative for A)
            q_snap = {b: queue_depth(attrs, b) for b in pool} if args.experiment == "queue" else {}

            ok, stamped, err = submit(manifest, arm, args.dry_run)
            if not ok:
                print(f"  [{arm} rep{rep}] submit FAILED: {err.strip()[:200]}")
                continue
            print(f"  [{arm} rep{rep}] submitted as {run_name}"
                  + (f"  (stamped={stamped})" if stamped else ""))

            if args.dry_run:
                continue

            rec = wait_and_collect(run_name, args.group_size, attrs)
            realized = rec.get("realized_backend") or stamped
            qw, lw, bl = derive(rec)
            cost = cost_for(attrs, realized, args.n_shots) if realized else None
            q_at_submit = q_snap.get(realized) if q_snap and realized else None

            rows.append({
                "experiment": args.experiment, "arm": arm, "policy":
                    (policy if arm != "baseline" else ""),
                "repeat": rep, "n_shots": args.n_shots, "group_size": args.group_size,
                "realized_backend": realized,
                "stamped_backend": stamped or "", "realized_cost_usd":
                    f"{cost:.4f}" if cost is not None else "",
                "queue_at_submit": q_at_submit if q_at_submit is not None else "",
                "qpu_queue_wait_s": f"{qw:.2f}" if qw is not None else "",
                "backend_latency_s": f"{bl:.2f}" if bl is not None else "",
                "leader_wall_s": f"{lw:.2f}" if lw is not None else "",
                "leader_phase": rec.get("leader_phase") or "",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            })
            cleanup(run_name, args.group_size)

    if rows and not args.dry_run:
        with open(out_csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        print(f"\nwrote {len(rows)} rows -> {out_csv}")
        _summary(rows)
    elif args.dry_run:
        print("\n(dry run: nothing applied, no CSV written)")
    else:
        print("\nNo rows collected.")


def _summary(rows):
    import statistics as st
    print("\nsummary:")
    by = {}
    for r in rows:
        by.setdefault(r["arm"], {"cost": [], "queue": [], "backends": {}})
        if r["realized_cost_usd"]:
            by[r["arm"]]["cost"].append(float(r["realized_cost_usd"]))
        if r["queue_at_submit"] != "":
            by[r["arm"]]["queue"].append(float(r["queue_at_submit"]))
        b = r["realized_backend"] or "?"
        by[r["arm"]]["backends"][b] = by[r["arm"]]["backends"].get(b, 0) + 1
    for arm, d in by.items():
        line = f"  {arm:<10}"
        if d["cost"]:
            m = st.mean(d["cost"]); s = st.stdev(d["cost"]) if len(d["cost"]) > 1 else 0
            line += f" cost ${m:.3f}±{s:.3f}"
        if d["queue"]:
            m = st.mean(d["queue"])
            line += f" queue~{m:.0f}"
        line += f"  backends={d['backends']}"
        print(line)


if __name__ == "__main__":
    main()
