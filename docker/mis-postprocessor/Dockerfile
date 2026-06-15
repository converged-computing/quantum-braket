FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="MIS postprocessor — greedy repair+grow on AHS measurement results"

WORKDIR /app
COPY postprocess.py .

ENV WORKSPACE=/workspace

CMD ["python", "postprocess.py"]
