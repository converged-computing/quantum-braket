FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="AHS gateway — submits MIS programs to QuEra Aquila or local AHS simulator"

WORKDIR /app

RUN pip install --no-cache-dir amazon-braket-sdk==1.88.0

COPY gateway_ahs.py .

ENV BRAKET_DEVICE=local
ENV N_SHOTS=100
ENV WORKSPACE=/workspace
ENV TOTAL_TIME=4.0

CMD ["python", "gateway_ahs.py"]
