FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="Role-aware producer/consumer gang workload (FLUENCE_COORDINATION_ROLE=producer|consumer)"

WORKDIR /app

RUN pip install --no-cache-dir \
    amazon-braket-sdk==1.88.0 \
    boto3

COPY gang.py .

# IMPORTANT: do NOT default FLUENCE_COORDINATION_ROLE. It must be unset in the
# image so the default (non-Fluence) arm can derive the role from
# JOB_COMPLETION_INDEX (index 0 = producer). Fluence injects it in the fluence
# arm. A baked-in default would make every default-arm pod a producer.
ENV N_CONSUMERS=4
ENV N_SHOTS=1000
# Waits default to "forever / very long" because a real QPU queue can be
# hours-to-days. CONSUMER_TIMEOUT_S<=0 = wait indefinitely for the producer's
# task id; POLL_TIMEOUT_S bounds the Braket result poll (30 days).
ENV CONSUMER_TIMEOUT_S=0
ENV POLL_TIMEOUT_S=2592000

CMD ["python", "gang.py"]
