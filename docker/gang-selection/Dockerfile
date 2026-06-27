FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="Selection gang container — self-contained (in-process problem bootstrap); role from FLUENCE_ROLE or GANG_ROLE"

WORKDIR /app

RUN pip install --no-cache-dir \
    amazon-braket-sdk==1.88.0 \
    boto3

COPY gang.py .

# Self-contained problem generation defaults (used by the in-process bootstrap
# when no problem-generator/transpiler init containers ran). Override as needed.
ENV N_NODES=10
ENV K_REGULAR=3
ENV SEED=42
ENV P_LAYERS=1

# Role is read from FLUENCE_ROLE (Fluence-injected) first, then GANG_ROLE.
# GANG_ROLE default kept for running outside Fluence (e.g. baseline arm).
ENV GANG_ROLE=worker
ENV N_WORKERS=1
ENV WORKER_INDEX=0
ENV N_SHOTS=100

CMD ["python", "gang.py"]
