FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="Gang workflow container — GANG_ROLE=leader or worker"

WORKDIR /app

RUN pip install --no-cache-dir \
    amazon-braket-sdk==1.88.0 \
    boto3

COPY gang.py .

ENV GANG_ROLE=worker
ENV N_WORKERS=4
ENV WORKER_INDEX=0
ENV N_SHOTS=1000
ENV WORKER_TIMEOUT_S=600
ENV LEADER_TIMEOUT_S=600

CMD ["python", "gang.py"]
