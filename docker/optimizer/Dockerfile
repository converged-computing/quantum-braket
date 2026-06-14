FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="QAOA optimizer — COBYLA variational loop over Braket circuit evaluations"

WORKDIR /app

RUN pip install --no-cache-dir \
    scipy==1.13.0 \
    numpy==1.26.4 \
    amazon-braket-sdk==1.88.0

COPY optimize.py .
COPY gateway.py .

ENV MAX_ITER=50
ENV TOL=1e-4
ENV GATEWAY_MODE=subprocess
ENV WORKSPACE=/workspace
ENV N_SHOTS=1000

CMD ["python", "optimize.py"]
