FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="Braket gateway — submits QAOA circuits to AWS Braket SV1"

WORKDIR /app

RUN pip install --no-cache-dir amazon-braket-sdk==1.88.0

COPY gateway.py .

ENV BRAKET_DEVICE=arn:aws:braket:::device/quantum-simulator/amazon/sv1
ENV N_SHOTS=1000
ENV WORKSPACE=/workspace
ENV ITERATION=0

CMD ["python", "gateway.py"]
