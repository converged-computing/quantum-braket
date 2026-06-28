# App containers built against Amazon EFA Open MPI

Four Dockerfiles: a shared EFA + Open MPI base, then AMG / LAMMPS / QMCPACK built
on top of it so MPI runs over the EFA fabric on Hpc7a.

## Why a shared base

The base installs the **aws-efa-installer**, which provides AWS's tuned libfabric
(/opt/amazon/efa) and Open MPI 4.1 (/opt/amazon/openmpi). Apps build with that
Open MPI's mpicc/mpicxx, so they link the EFA libfabric and use the `ofi` MTL at
run time. This is AWS's recommended pattern (vs. building a generic Open MPI),
and it's what "compile against Amazon EFA Open MPI" means.

We use the installer's Open MPI, NOT a Spack/distro Open MPI. (The 2022 EFA
Dockerfiles built Open MPI via Spack; the installer route is simpler and is the
AWS-supported stack.)

## Build

```bash
GHCR=ghcr.io/converged-computing
docker build -f Dockerfile.efa-base -t $GHCR/efa-openmpi-base:noble .
docker build -f Dockerfile.amg     --build-arg BASE=$GHCR/efa-openmpi-base:noble -t $GHCR/amg-efa:latest .
docker build -f Dockerfile.lammps  --build-arg BASE=$GHCR/efa-openmpi-base:noble -t $GHCR/lammps-efa:latest .
docker build -f Dockerfile.qmcpack --build-arg BASE=$GHCR/efa-openmpi-base:noble -t $GHCR/qmcpack-efa:latest .

kind load docker-image $GHCR/qmcpack-efa:latest
kind load docker-image $GHCR/amg-efa:latest
kind load docker-image $GHCR/lammps-efa:latest
```

For Hpc7a (AMD Genoa / Zen4) you can add `-march=znver4` to app C/CXX flags for a
small gain; left out by default for portability.

## Verify EFA + MPI actually work (run on an EFA-capable host/pod)

    fi_info -p efa                         # must list provider: efa
    ompi_info --parsable --all | grep mtl  # must show mtl:ofi (libfabric/EFA path)

If `fi_info` shows only sockets/tcp, the container can't see the EFA device —
check the pod is requesting the EFA resource and the node has the kernel module.

## Running these as Flux Operator MiniClusters

The Flux Operator stages a Flux view and runs the container command under Flux.
Point each app's MiniCluster `containers[].image` at the images above. Two EFA
specifics the MiniCluster/pod must satisfy on Hpc7a:

1. EFA device resource: request `vpc.amazonaws.com/efa: 1` on the container
   (exposed by the EFA device plugin on the node), so the pod gets the EFA
   interface.
2. hugepages + memlock: EFA needs locked memory; ensure the pod's ulimits/limits
   allow it (the node's EFA setup normally handles memlock=unlimited).

These are runtime/scheduling concerns, separate from the build. The Dockerfiles
above only guarantee the binaries are BUILT against the EFA Open MPI; the pod
spec must then expose the EFA device for them to USE it.

## App specifics

- AMG: LLNL/AMG branch 1.2, `make CC=mpicc`. Run: amg -P 4 2 2 -n 128 128 128 (16 ranks).
- LAMMPS: tag stable_29Sep2021_update3, ReaxFF + FFTW3. Run: lmp -in in.reaxc.hns
  (input copied to /opt/hns from examples/reaxff/HNS).
- QMCPACK: v3.13.0, SoA + mixed precision. NiO benchmark input (.h5 + .xml) is
  large and NOT baked in — stage it as a volume and run `qmcpack <NiO>.xml`
  (188 ranks across 2 pods in the 2022 run).

## Honest status

Base OS is Ubuntu 24.04 (noble) — the current LTS, and supported by the AWS EFA
installer (AWS supports AL2023, AL2, Ubuntu 24.04, and 22.04). Note the apps
pinned here are older (AMG 1.2, LAMMPS Sep2021, QMCPACK 3.13.0) and 24.04 ships
GCC 14, which is stricter than the GCC the 2022 builds used. The EFA layer is
fine on 24.04; the RISK is the app compiles hitting newer-compiler errors
(implicit declarations, default -std bumps). If an app fails to build on 24.04
for a compiler reason, the fix is per-app (add -fcommon / -std=c++14 / a small
patch), or pin that one app's base to ubuntu:22.04 (also EFA-supported). Do NOT
drop below 22.04 — 20.04 is past EFA support.

These recipes combine (a) the exact app sources/branches/cmake flags from the
2022 artifacts' working EFA builds and (b) AWS's documented efa-installer base
pattern. They have NOT been built here (no Docker daemon; the installer pulls
from AWS at build time). Treat the first `docker build` of each as the real
verification. Most likely first-build snags, all app-side not EFA-side:
- AMG branch 1.2 Makefile may need `CC=mpicc` passed (done) or a small flag tweak.
- LAMMPS cmake occasionally needs `-D CMAKE_BUILD_TYPE=Release`; add if it warns.
- QMCPACK needs boost/fftw/hdf5/blas/lapack (in the base) — if cmake can't find
  HDF5, add `-DHDF5_ROOT=/usr/lib/x86_64-linux-gnu/hdf5/serial`.
