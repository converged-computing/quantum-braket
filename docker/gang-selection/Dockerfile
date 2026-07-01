FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="Selection gang container — unified gang+submitter path (no leader/worker); self-contained in-process problem bootstrap"

WORKDIR /app

RUN pip install --no-cache-dir \
    amazon-braket-sdk==1.88.0 \
    boto3

COPY gang.py .

# Self-contained problem generation defaults (used by the in-process bootstrap).
ENV N_NODES=10
ENV K_REGULAR=3
ENV SEED=42
ENV P_LAYERS=1
ENV N_SHOTS=100

# No role env: gang.py branches only on FLUENCE_COORDINATION_ROLE, which Fluence
# sets on consumers (-> fetch) vs the producer/index-0 (-> real submit).
CMD ["python", "gang.py"]
