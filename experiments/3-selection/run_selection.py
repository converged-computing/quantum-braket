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
    realized_backend   the backend the producer ran on (from its TIMING log
                       or the stamped annotation)
    realized_cost_usd  computed from the attribute file + shot count
    queue_at_submit    queue depth of the chosen device at submit (sub-exp B)
    producer_wall_s, qpu_queue_wait_s   timing (same derivation as Experiment 2)

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
POOL_COST = ["sv1", "dm1", "rigetti_cepheus", "iqm_garnet", "iqm_emerald"]
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
    """Render the gang as a single batch/v1 Job (parallelism=group_size). The
    select-* block (selection arm) goes on the JOB's top-level annotations, where
    `kubectl fluence` reads the policy; baseline removes it. Fluence derives the
    gang name (Job name) and gang size (parallelism) from the Job owner — no group
    label or group-size annotation needed.
    """
    with open(TEMPLATE) as f:
        m = f.read()
    if select_block.strip():
        job_ann = "  annotations:\n" + select_block.rstrip("\n")
    else:
        job_ann = ""
    repl = {
        "__JOB_ANNOTATIONS__": job_ann,
        "__RUN_NAME__": run_name,
        "__GROUP_SIZE__": str(group_size),
        "__RUN_ID__": run_id,
        "__N_SHOTS__": str(n_shots),
        "__SCHEDULER__": "fluence",
    }
    for k, v in repl.items():
        m = m.replace(k, v)
    return m


def choose_random(pool, seed):
    """The BASELINE chooser: a pseudo-random pick from the SAME candidate pool the
    policy ranks over. Seeded by the run id (stable + logged) so each repeat draws
    independently and the draw is reproducible from the recorded run. This is the
    control the policy must beat — not Fluence's capability default."""
    import random
    return random.Random(seed).choice(pool)


def pin_backend(manifest, name, arn):
    """Pin the chosen backend as a require-backend CONSTRAINT on the pod template.
    Fluxion then matches the producer's group-of-one to that backend (require-* is
    collected per the pod's own group; the producer carries the template's
    annotations) and injects FLUXION_BACKEND from the matched backend, which
    gang.py resolves to the ARN. We ALSO bake FLUXION_ARN/FLUXION_BACKEND into the
    env as a robustness belt (same value), so honoring holds even if a cluster's
    attribute injection is misconfigured. BOTH arms pin through this identical
    path; only the CHOICE of `name` differs (random baseline vs policy)."""
    import yaml
    obj = yaml.safe_load(manifest)
    anns = obj["spec"]["template"]["metadata"].setdefault("annotations", {})
    anns["fluence.flux-framework.org/require-backend"] = name
    for c in obj["spec"]["template"]["spec"]["containers"]:
        env = c.setdefault("env", [])
        names = {e.get("name") for e in env}
        if "FLUXION_ARN" not in names and arn:
            env.append({"name": "FLUXION_ARN", "value": arn})
        if "FLUXION_BACKEND" not in names and name:
            env.append({"name": "FLUXION_BACKEND", "value": name})
    return yaml.safe_dump(obj, sort_keys=False)


def submit(manifest, arm, dry_run, attrs, pool, seed):
    """Submit the gang Job. BOTH arms pin the chosen backend as a require-backend
    constraint on the pod template (same path); only the CHOOSER differs:

      baseline  -> choose_random(pool, seed): a seeded random pick from the
                   candidate pool. This is the control — NOT Fluence's capability
                   default ("first in graph"), which would be an unfair baseline.
      selection -> `kubectl fluence select` resolves the policy (min-cost /
                   online-only,min-queue) from the Job's select-* block.

    Returns (ok, chosen_backend_or_None, stderr_text).
    """
    stderr = ""
    if arm == "baseline":
        chosen = choose_random(pool, seed)
    else:
        sel = subprocess.run(
            ["kubectl", "fluence", "select", "-n", NAMESPACE,
             "--attributes", ATTR_FILE, "-f", "-"],
            input=manifest, text=True, capture_output=True)
        if sel.returncode != 0:
            return False, None, sel.stderr
        stderr = sel.stderr
        chosen = _backend_from_stderr(sel.stderr) or _parse_stamped_backend(sel.stdout)
        if not chosen:
            return False, None, sel.stderr or "no backend resolved"

    arn = (attrs.get(chosen) or {}).get("device_arn", "") if chosen else ""
    pinned = pin_backend(manifest, chosen, arn)

    if dry_run:
        print(f"--- DRY RUN ({arm}) -> require-backend={chosen}  arn={arn or '(unknown)'} ---")
        if stderr.strip():
            print(stderr.rstrip())
        return True, chosen, stderr

    r = subprocess.run(["kubectl", "apply", "-n", NAMESPACE, "-f", "-"],
                       input=pinned, text=True, capture_output=True)
    return (r.returncode == 0), chosen, (stderr + r.stderr)


def _backend_from_stderr(stderr):
    """Parse `selected backend "X" for ...` from the plugin's stderr."""
    for line in stderr.splitlines():
        s = line.strip()
        if s.startswith("selected backend "):
            # selected backend "sv1" for Job/sel-... (policy "min-cost")
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
# realized backend + timing readback (from the producer pod's TIMING logs)
# ----------------------------------------------------------------------------
def wait_and_collect(run_name, group_size, attrs=None, poll_s=15, heartbeat_s=120):
    """Wait for the gang to finish — for as long as it takes. A quantum task can
    sit in the vendor queue for 30-60+ minutes, so there is NO timeout: we poll
    the producer pod until it reaches a terminal phase (Succeeded/Failed). A
    heartbeat line is logged periodically so a long, healthy queue wait is
    visibly alive rather than looking hung. Returns realized backend + timings
    parsed from the producer's logs.

    To interrupt a run, use Ctrl-C (KeyboardInterrupt) — that is the deliberate
    stop, not an automatic deadline that could kill a run that is merely slow.
    """
    pod0 = None                      # the producer pod (completion index 0)
    phase = None
    start = time.time()
    next_heartbeat = start + heartbeat_s
    while True:
        if pod0 is None:
            pod0 = _producer_pod(run_name)
        if pod0:
            phase = _pod_phase(pod0)
            if phase in ("Succeeded", "Failed"):
                break
        now = time.time()
        if now >= next_heartbeat:
            waited = int(now - start)
            print(f"    [{run_name}] still waiting ({waited // 60}m{waited % 60}s) "
                  f"— producer={pod0 or 'pending'} phase={phase} "
                  f"(quantum queue can take 30-60+ min)")
            next_heartbeat = now + heartbeat_s
        time.sleep(poll_s)
    logs = _pod_logs(pod0)
    rec = _parse_timing(logs, attrs)
    rec["producer_phase"] = phase
    return rec


def _producer_pod(run_name):
    """Resolve the producer pod = the Indexed-Job completion-index-0 pod (Fluence
    promotes index 0 to the producer; its name carries a random suffix, so we
    look it up by label rather than construct it)."""
    try:
        out = subprocess.check_output(
            ["kubectl", "get", "pods", "-n", NAMESPACE,
             "-l", f"job-name={run_name},batch.kubernetes.io/job-completion-index=0",
             "-o", "jsonpath={.items[0].metadata.name}"], stderr=subprocess.DEVNULL)
        return out.decode().strip() or None
    except Exception:
        return None


def _pod_phase(pod):
    try:
        out = subprocess.check_output(
            ["kubectl", "get", "pod", pod, "-n", NAMESPACE,
             "-o", "jsonpath={.status.phase}"], stderr=subprocess.DEVNULL)
        return out.decode().strip()
    except Exception:
        return None


def _pod_logs(pod):
    if not pod:
        return ""
    try:
        return subprocess.check_output(
            ["kubectl", "logs", pod, "-n", NAMESPACE, "-c", "gang"],
            stderr=subprocess.DEVNULL).decode()
    except Exception:
        try:
            return subprocess.check_output(
                ["kubectl", "logs", pod, "-n", NAMESPACE],
                stderr=subprocess.DEVNULL).decode()
        except Exception:
            return ""


def _parse_timing(logs, attrs=None):
    """Extract realized backend + timestamps from the producer's log format.

    gang.py (producer path) emits:  TIMING <key>_ts=<epoch>
      start_ts, submit_ts, queued_ts, result_ts, end_ts
    and the realized backend on two lines:
      FLUXION_BACKEND=<name>
      device=<arn>
    Prefer the FLUXION_BACKEND name; else map the device= ARN to a name.
    """
    rec = {"realized_backend": None, "t_queued": None, "t_result": None,
           "t_start": None, "t_end": None, "t_submit": None}
    arn_to_name = {}
    if attrs:
        for name, b in attrs.items():
            if b.get("device_arn"):
                arn_to_name[b["device_arn"]] = name

    ts_map = {
        "start_ts": "t_start",
        "submit_ts": "t_submit",
        "queued_ts": "t_queued",
        "result_ts": "t_result",
        "end_ts": "t_end",
    }
    for line in logs.splitlines():
        if "TIMING" in line:
            for tok in line.split():
                if "_ts=" in tok:
                    k, _, v = tok.partition("=")
                    if k in ts_map:
                        try:
                            rec[ts_map[k]] = float(v)
                        except ValueError:
                            pass
        if "FLUXION_BACKEND=" in line:
            for tok in line.split():
                if tok.startswith("FLUXION_BACKEND="):
                    val = tok.split("=", 1)[1].strip()
                    if val:
                        rec["realized_backend"] = val
        if rec["realized_backend"] is None and "device=" in line:
            for tok in line.split():
                if tok.startswith("device="):
                    dev = tok.split("=", 1)[1].strip()
                    rec["realized_backend"] = arn_to_name.get(dev, dev)
    return rec


def derive(rec):
    """Return (qpu_queue_wait_s, producer_wall_s, backend_latency_s).
    - qpu_queue_wait_s: queued -> result (time in the vendor queue + run)
    - producer_wall_s: start -> end (whole producer lifetime)
    - backend_latency_s: submit -> result (full backend turnaround), the "LONG
      time" you observe waiting on the device.
    """
    qw = None
    if rec.get("t_result") and rec.get("t_queued"):
        qw = rec["t_result"] - rec["t_queued"]
    lw = None
    if rec.get("t_end") and rec.get("t_start"):
        lw = rec["t_end"] - rec["t_start"]
    bl = None
    if rec.get("t_result") and rec.get("t_submit"):
        bl = rec["t_result"] - rec["t_submit"]
    return qw, lw, bl


# ----------------------------------------------------------------------------
# cleanup
# ----------------------------------------------------------------------------
def cleanup(run_name, group_size):
    # Delete the Job (cascades to ALL gang pods, including the producer) + the
    # PodGroups Fluence created: the <run> consumer gang and the producer's
    # group-of-one <run>-producer. Keeps a run from lingering on qpu slots.
    subprocess.run(["kubectl", "delete", "job", "-n", NAMESPACE,
                    "--ignore-not-found", run_name], capture_output=True)
    subprocess.run(["kubectl", "delete", "podgroup", "-n", NAMESPACE,
                    "--ignore-not-found", run_name, f"{run_name}-producer"],
                   capture_output=True)


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
                    help="gang size = Indexed-Job parallelism (default 2). Fluence promotes completion index 0 to the producer (single real submit to the selected backend) and gates the other N-1 as consumers that fetch the result.")
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
              "backend_latency_s", "producer_wall_s", "producer_phase", "timestamp"]
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

            ok, stamped, err = submit(manifest, arm, args.dry_run, attrs, pool, run_id)
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
                    (policy if arm != "baseline" else "random"),
                "repeat": rep, "n_shots": args.n_shots, "group_size": args.group_size,
                "realized_backend": realized,
                "stamped_backend": stamped or "", "realized_cost_usd":
                    f"{cost:.4f}" if cost is not None else "",
                "queue_at_submit": q_at_submit if q_at_submit is not None else "",
                "qpu_queue_wait_s": f"{qw:.2f}" if qw is not None else "",
                "backend_latency_s": f"{bl:.2f}" if bl is not None else "",
                "producer_wall_s": f"{lw:.2f}" if lw is not None else "",
                "producer_phase": rec.get("producer_phase") or "",
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
