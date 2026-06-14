FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="QAOA problem generator"

WORKDIR /app
COPY generate.py .

ENV N_NODES=10
ENV K_REGULAR=3
ENV SEED=42
ENV WORKSPACE=/workspace

CMD ["python", "generate.py"]
