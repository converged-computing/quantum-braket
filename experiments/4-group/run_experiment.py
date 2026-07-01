#!/usr/bin/env python3
"""
run_contention_experiment.py — the real gang-scheduling test: submit MANY gangs
of DIFFERENT apps and sizes at once into a pool too small to hold them, and watch
how each scheduler copes. This is the production scenario gang scheduling exists
for, and where the default scheduler fails (partial placement / deadlock: a gang
gets some pods on nodes and holds them while waiting for the rest, blocking other
gangs) while Fluence places whole gangs or none.

A BATCH is a mix across apps x sizes (the grid), all submitted concurrently.
Each gang is a MiniCluster (one pod per node, gang = its pod set).

Arms: default-scheduler first, then Fluence.

Per gang we record: when it became fully placed (all pods scheduled), fully
ready, finished; its status (placed / partial / pending / done / failed); and
its node layout. Per batch we record makespan and how many ever placed.
"""
import argparse, csv, json, os, random, subprocess, time, uuid
from datetime import datetime, timezone

NS = "default"
GROUP_LABEL = "fluence.flux-framework.org/group"
FLUX_VIEW = "ghcr.io/converged-computing/flux-view-ubuntu:tag-noble"
RANKS_PER_POD = 8
CPUS_PER_POD = 80    # request ~a whole h3-standard-88 node -> one pod per node


def amg_topology(total):
    best = (1, 1, total); a = 1
    while a * a * a <= total:
        if total % a == 0:
            rem = total // a; b = a
            while b * b <= rem:
                if rem % b == 0:
                    best = (a, b, rem // b)
                b += 1
        a += 1
    return f"{best[0]} {best[1]} {best[2]}"


APPS = {
    "amg":     {"image": "vanessa/fluence-experiments:amg",
                "cmd": lambda r: f"amg -P {amg_topology(r)} -n 8 8 8", "workdir": None, "imagePullPolicy": "Always"},
    "lammps":  {"image": "vanessa/fluence-experiments:lammps",
                "cmd": lambda r: "lmp -v x 8 -v y 4 -v z 8 -v nsteps 5 -in in.reaxc.hns -nocite",
                "workdir": "/opt/hns", "imagePullPolicy": "Always"},
    # QMCPACK: set the command to the PLAIN VMC input from /opt/smoke (NOT the
    # h2_orb_opt optimizer, which is slow). Replace VMC_INPUT.xml with the file
    # you found (e.g. confirm via: ls /opt/smoke in the qmcpack image).
    "qmcpack": {"image": "vanessa/fluence-experiments:qmcpack-hamiltonian", "imagePullPolicy": "Always",
                "cmd": lambda r: "qmcpack h2_vmc.xml", "workdir": "/opt/smoke"},
}


def kubectl(*a, check=True):
    return subprocess.run(["kubectl", *a], check=check, capture_output=True, text=True)


def now():
    return datetime.now(timezone.utc)


def build_batch(apps, sizes):
    """The grid: one gang per (app, size). All submitted at once."""
    batch = []
    for app in apps:
        for i in range(1):
            for size in sizes:
                batch.append({"app": app, "size": size,
                              "name": f"{app}-n{size}-{uuid.uuid4().hex[:6]}"})
    print(batch)
    random.shuffle(batch)   # interleave apps/sizes so submission order is mixed
    return batch


def render(g, sched, args_cpus):
    app, size, name = g["app"], g["size"], g["name"]
    cfg = APPS[app]
    ranks = size * RANKS_PER_POD
    pod = {"labels": {"batch.gang/name": name}}
    if sched == "fluence":
        pod["schedulerName"] = "fluence"
        pod["labels"][GROUP_LABEL] = name
    # topology is controlled by the CPU request (one ~node-sized pod per node),
    # NOT by affinity — so we disable the operator's default anti-affinity below.
    c = {"image": cfg["image"], "imagePullPolicy": "Always",
         "command": cfg["cmd"](ranks),
         "resources": {"requests": {"cpu": str(args_cpus), "memory": "8Gi"},
                       "limits": {"cpu": str(args_cpus), "memory": "8Gi"}},
         "volumes": {"shared-memory": {"emptyDir": True, "emptyDirMedium": "memory"}}}
    if cfg["workdir"]:
        c["workingDir"] = cfg["workdir"]
    return {"apiVersion": "flux-framework.org/v1alpha2", "kind": "MiniCluster",
            "metadata": {"name": name, "namespace": NS},
            "spec": {"size": size, "tasks": ranks,
                     "pod": pod,
                     "flux": {"container": {"image": FLUX_VIEW}}, "containers": [c]}}


def gang_pods(name):
    out = kubectl("get", "pods", "-n", NS, "-l", f"batch.gang/name={name}",
                  "-o", "json", check=False).stdout
    try:
        return json.loads(out).get("items", [])
    except Exception:
        return []


def parse_ts(x):
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if x else None


def gang_timestamps(pods):
    """Real timestamps across a gang's pods:
       created   = earliest pod creationTimestamp (submit/admission)
       scheduled = latest PodScheduled time (whole gang placed)
       ready     = latest Ready time (whole gang up; Flux quorum formed)
       app_start = earliest container start (app actually began)
       app_end   = latest container finish (app done)
    Latest-across-pods for scheduled/ready/end because the gang is a unit; the
    gang isn't 'placed' until its LAST pod is, and not 'done' until its last
    container exits. app_start is earliest (first rank to start)."""
    created, scheduled, ready, starts, ends = [], [], [], [], []
    for p in pods:
        m, st = p.get("metadata", {}), p.get("status", {})
        created.append(parse_ts(m.get("creationTimestamp")))
        for c in st.get("conditions", []):
            if c.get("type") == "PodScheduled" and c.get("status") == "True":
                scheduled.append(parse_ts(c.get("lastTransitionTime")))
            if c.get("type") == "Ready" and c.get("status") == "True":
                ready.append(parse_ts(c.get("lastTransitionTime")))
        for cs in st.get("containerStatuses", []):
            stt = cs.get("state", {})
            run = stt.get("running") or stt.get("terminated")
            if run and run.get("startedAt"):
                starts.append(parse_ts(run["startedAt"]))
            if "terminated" in stt and stt["terminated"].get("finishedAt"):
                ends.append(parse_ts(stt["terminated"]["finishedAt"]))
    def mx(v): 
        v = [x for x in v if x]; return max(v) if v else None
    def mn(v):
        v = [x for x in v if x]; return min(v) if v else None
    return dict(created=mn(created), scheduled=mx(scheduled), ready=mx(ready),
                app_start=mn(starts), app_end=mx(ends))


def pod_snapshot(p):
    """Extract the durable per-pod timestamps from a single pod object.
    creationTimestamp / PodScheduled / container start+end all PERSIST into the
    terminal snapshot; Ready=True does NOT (it flips to False on exit), so it must
    be captured while the pod is alive -- see update_pod_recs()."""
    m, st = p.get("metadata", {}), p.get("status", {})
    rec = {"created": parse_ts(m.get("creationTimestamp")),
           "scheduled": None, "ready": None, "app_start": None, "app_end": None,
           "node": p.get("spec", {}).get("nodeName") or None}
    for c in st.get("conditions", []):
        if c.get("type") == "PodScheduled" and c.get("status") == "True":
            rec["scheduled"] = parse_ts(c.get("lastTransitionTime"))
        if c.get("type") == "Ready" and c.get("status") == "True":
            rec["ready"] = parse_ts(c.get("lastTransitionTime"))
    for cs in st.get("containerStatuses", []):
        stt = cs.get("state", {})
        run = stt.get("running") or stt.get("terminated")
        if run and run.get("startedAt"):
            rec["app_start"] = parse_ts(run["startedAt"])
        if "terminated" in stt and stt["terminated"].get("finishedAt"):
            rec["app_end"] = parse_ts(stt["terminated"]["finishedAt"])
    return rec


def update_pod_recs(store, pods):
    """Accumulate per-pod records ACROSS polls into `store` (keyed by pod name),
    keeping the first non-null value seen for each field. This is what makes
    ready_at survive: we latch Ready=True the poll we first see it, before the
    pod terminates and the condition flips to False. All other fields are stable
    historical times, so first-seen == authoritative."""
    for p in pods:
        name = p.get("metadata", {}).get("name")
        if not name:
            continue
        cur = store.setdefault(name, {})
        for k, v in pod_snapshot(p).items():
            if v is not None and cur.get(k) is None:
                cur[k] = v


def gang_metrics(recs):
    """Exact gang-level metrics from per-pod records (recs = list of per-pod dicts).
    Everything here is derived from real per-pod times and is sign-correct:
       created        = earliest pod creation (submit/admission)
       scheduled      = LATEST PodScheduled (whole gang placed)
       ready          = LATEST Ready=True (whole gang reported ready)
       first_running  = EARLIEST container start (first rank up)
       gang_up        = LATEST container start (ALL ranks up -> coupled app can
                        actually make progress; this is the "entire gang running"
                        marker)
       app_end        = LATEST container finish (gang done)
       worker_idle_node_s = SUM over pods of (gang_up - pod_start): exact
                        node-seconds a rank held a node while waiting for the
                        slowest rank to come up. >=0 by construction. Replaces the
                        old sign-flipping skew*(size-1) upper bound.
       assembly_span_s = gang_up - first_running (stagger between first & last up)
       run_s          = app_end - gang_up (whole gang up -> finished; the true
                        coupled run time, excludes queue wait AND assembly idle)"""
    def mx(k):
        v = [r[k] for r in recs if r.get(k)]; return max(v) if v else None
    def mn(k):
        v = [r[k] for r in recs if r.get(k)]; return min(v) if v else None
    created, scheduled, ready = mn("created"), mx("scheduled"), mx("ready")
    first_running, gang_up, app_end = mn("app_start"), mx("app_start"), mx("app_end")
    starts = [r["app_start"] for r in recs if r.get("app_start")]
    worker_idle = None
    if gang_up and starts:
        worker_idle = round(sum((gang_up - s).total_seconds() for s in starts), 2)
    return dict(created=created, scheduled=scheduled, ready=ready,
                first_running=first_running, gang_up=gang_up, app_end=app_end,
                worker_idle_node_s=worker_idle)


def counts(pods):
    sched = ready = done = 0
    for p in pods:
        st = p.get("status", {})
        for c in st.get("conditions", []):
            if c.get("type") == "PodScheduled" and c.get("status") == "True":
                sched += 1
            if c.get("type") == "Ready" and c.get("status") == "True":
                ready += 1
        if st.get("phase") in ("Succeeded", "Failed"):
            done += 1
    return sched, ready, done


def node_layout(pods):
    layout = {}
    for p in pods:
        n = p.get("spec", {}).get("nodeName") or "<pending>"
        layout[n] = layout.get(n, 0) + 1
    return layout


def save_logs(name, path):
    for p in gang_pods(name):
        pn = p["metadata"]["name"]
        out = kubectl("logs", "-n", NS, pn, "--all-containers=true", check=False).stdout
        with open(f"{path}/{pn}.log", "w") as f:
            f.write(out)


def run_arm(sched, batch, args):
    arm_dir = f"{args.out_dir}/{sched}"
    os.makedirs(arm_dir, exist_ok=True)
    import yaml

    # submit the WHOLE batch at once
    t0 = now()
    for g in batch:
        mf = f"/tmp/{g['name']}.yaml"
        with open(mf, "w") as f:
            yaml.safe_dump(render(g, sched, args.cpus_per_pod), f)
        res = kubectl("apply", "-f", mf, check=False)
        g["manifest"] = mf
        g.update(placed_at=None, ready_at=None, done_at=None, status="pending",
                 pod_recs={})
        if res.returncode != 0:
            g["status"] = "apply_failed"
            print(f"  ! apply FAILED for {g['name']}: {res.stderr.strip().splitlines()[-1] if res.stderr.strip() else res.stdout.strip()}")
    ok = [g for g in batch if g["status"] != "apply_failed"]
    print(f"[{sched}] applied {len(ok)}/{len(batch)} gangs "
          f"({sum(g['size'] for g in ok)} pods) into {args.nodes} nodes")
    # verify what actually got created
    mc = kubectl("get", "miniclusters", "-n", NS, "-o", "name", check=False).stdout
    print(f"[{sched}] miniclusters now present: {len([x for x in mc.splitlines() if x.strip()])}")

    # poll the whole set until all terminal or the batch times out
    deadline = time.time() + args.batch_timeout
    while time.time() < deadline:
        pending = [g for g in batch if g["status"] not in ("done", "failed", "apply_failed")]
        if not pending:
            break
        for g in pending:
            pods = gang_pods(g["name"])
            update_pod_recs(g["pod_recs"], pods)   # latch per-pod times WHILE alive
            sched_n, ready_n, done_n = counts(pods)
            if g["placed_at"] is None and sched_n >= g["size"]:
                g["placed_at"] = now()                       # whole gang placed
            if g["ready_at"] is None and ready_n >= g["size"]:
                g["ready_at"] = now()
            if done_n >= g["size"]:
                g["done_at"] = now()
                anyfail = any(p.get("status", {}).get("phase") == "Failed" for p in pods)
                g["status"] = "failed" if anyfail else "done"
                # grab logs + final pod snapshot NOW, before teardown can remove them
                save_logs(g["name"], f"{args.out_dir}/{sched}")
                g["final_pods"] = pods
            elif sched_n >= g["size"]:
                g["status"] = "placed"
            elif sched_n > 0:
                g["status"] = "partial"                      # the default-sched failure mode
            else:
                g["status"] = "pending"
        time.sleep(args.poll)

    # build rows from REAL pod/container timestamps (stage DURATIONS, not offsets)
    def dur(a, b):
        return round((b - a).total_seconds(), 2) if (a and b) else ""
    rows = []
    pod_rows = []
    for g in batch:
        pods = g.get("final_pods") or gang_pods(g["name"])
        if not g.get("final_pods"):                 # not terminal: grab logs now
            save_logs(g["name"], arm_dir)
        update_pod_recs(g["pod_recs"], pods)         # final latch from last snapshot
        layout = node_layout(pods)
        recs = list(g["pod_recs"].values())
        m = gang_metrics(recs)
        # legacy gang-level view (kept for back-compat with any old tooling)
        ts = gang_timestamps(pods)
        # stage durations, now from sign-correct per-pod metrics:
        pending_s = dur(m["created"], m["scheduled"])    # submit -> gang placed
        startup_s = dur(m["scheduled"], m["ready"])       # placed -> all ready
        apprun_s  = dur(m["first_running"], m["app_end"]) # first rank up -> done
        total_s   = dur(m["created"], m["app_end"])       # admission -> app done
        # NEW exact metrics (the ones the paper should use):
        assembly_span_s = dur(m["first_running"], m["gang_up"])  # first up -> all up
        run_s           = dur(m["gang_up"], m["app_end"])        # WHOLE GANG UP -> done
        worker_idle_node_s = m["worker_idle_node_s"]             # exact, >=0
        placed_after_s = dur(t0, m["scheduled"])
        done_after_s   = dur(t0, m["app_end"])
        rows.append(dict(
            scheduler=sched, gang=g["name"], app=g["app"],
            size=g["size"], ranks=g["size"] * RANKS_PER_POD,
            final_status=g["status"],
            pending_s=pending_s, startup_s=startup_s, apprun_s=apprun_s, total_s=total_s,
            assembly_span_s=assembly_span_s, run_s=run_s,
            worker_idle_node_s=worker_idle_node_s,
            placed_after_s=placed_after_s, done_after_s=done_after_s,
            created_at=m["created"].isoformat() if m["created"] else "",
            scheduled_at=m["scheduled"].isoformat() if m["scheduled"] else "",
            ready_at=m["ready"].isoformat() if m["ready"] else "",
            first_running_at=m["first_running"].isoformat() if m["first_running"] else "",
            gang_up_at=m["gang_up"].isoformat() if m["gang_up"] else "",
            app_start_at=m["first_running"].isoformat() if m["first_running"] else "",
            app_end_at=m["app_end"].isoformat() if m["app_end"] else "",
            nodes=len([n for n in layout if n != "<pending>"]),
            node_layout=json.dumps(layout)))
        # PER-POD rows: everything needed to recompute any gang metric offline
        for pname, r in sorted(g["pod_recs"].items()):
            pod_rows.append(dict(
                scheduler=sched, gang=g["name"], app=g["app"], size=g["size"],
                pod=pname, node=r.get("node") or "",
                created_at=r["created"].isoformat() if r.get("created") else "",
                scheduled_at=r["scheduled"].isoformat() if r.get("scheduled") else "",
                ready_at=r["ready"].isoformat() if r.get("ready") else "",
                app_start_at=r["app_start"].isoformat() if r.get("app_start") else "",
                app_end_at=r["app_end"].isoformat() if r.get("app_end") else ""))
        kubectl("delete", "-f", g["manifest"], "--ignore-not-found", "--wait=false", check=False)
    for g in batch:
        kubectl("wait", "--for=delete", "pod", "-n", NS,
                "-l", f"batch.gang/name={g['name']}", "--timeout=180s", check=False)

    placed = [g for g in batch if g["status"] in ("placed", "done")]
    makespan = max([r["done_after_s"] for r in rows if r["done_after_s"] != ""] or [""])
    print(f"[{sched}] placed {len(placed)}/{len(batch)} gangs; "
          f"partial/pending: {sum(1 for g in batch if g['status'] in ('partial','pending'))}; "
          f"makespan={makespan}")
    return rows, pod_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--arms", nargs="+", default=["default", "fluence"],
                    choices=["default", "fluence"])
    ap.add_argument("--apps", nargs="+", default=list(APPS.keys()))
    ap.add_argument("--sizes", nargs="+", type=int, default=[1, 2, 4],
                    help="gang sizes in the grid (pods); the batch is apps x sizes")
    ap.add_argument("--reps", type=int, default=1, help="repeat the whole batch")
    ap.add_argument("--nodes", type=int, default=4, help="cluster size (for logging/contention math)")
    ap.add_argument("--cpus-per-pod", type=int, default=CPUS_PER_POD,
                    help="CPU request per pod; size it to ~a node so topology is one-pod-per-node")
    ap.add_argument("--batch-timeout", type=int, default=1800)
    ap.add_argument("--poll", type=int, default=5)
    ap.add_argument("--out-dir", default="results")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    fields = ["scheduler", "rep", "gang", "app", "size", "ranks", "final_status",
              "pending_s", "startup_s", "apprun_s", "total_s",
              "assembly_span_s", "run_s", "worker_idle_node_s",
              "placed_after_s", "done_after_s",
              "created_at", "scheduled_at", "ready_at",
              "first_running_at", "gang_up_at", "app_start_at", "app_end_at",
              "nodes", "node_layout"]
    pod_fields = ["scheduler", "rep", "gang", "app", "size", "pod", "node",
                  "created_at", "scheduled_at", "ready_at",
                  "app_start_at", "app_end_at"]
    rows = []
    pod_rows = []
    for sched in args.arms:
        print(f"\n=== scheduler arm: {sched} ===")
        for rep in range(1, args.reps + 1):
            batch = build_batch(args.apps, args.sizes)
            demand = sum(g["size"] for g in batch)
            print(f"--- rep {rep}: {len(batch)} gangs, {demand} pods vs {args.nodes} nodes "
                  f"({'CONTENDED' if demand > args.nodes else 'fits'}) ---")
            grows, prows = run_arm(sched, batch, args)
            for r in grows:
                r["rep"] = rep
                rows.append(r)
            for r in prows:
                r["rep"] = rep
                pod_rows.append(r)

    csv_path = f"{args.out_dir}/contention.csv"
    hdr = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if hdr:
            w.writeheader()
        w.writerows(rows)
    pod_path = f"{args.out_dir}/contention_pods.csv"
    phdr = not os.path.exists(pod_path)
    with open(pod_path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=pod_fields)
        if phdr:
            w.writeheader()
        w.writerows(pod_rows)
    print(f"\nwrote {csv_path} (+{len(rows)} gang rows) and {pod_path} "
          f"(+{len(pod_rows)} pod rows); logs under {args.out_dir}/<arm>/")


if __name__ == "__main__":
    main()
