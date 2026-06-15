FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="AHS problem generator — unit disk graph for MIS via QuEra Aquila"

WORKDIR /app
COPY generate_ahs.py .

ENV N_ATOMS=16
ENV DROPOUT=0.3
ENV SEED=42
ENV LATTICE_SPACING=5.5e-6
ENV WORKSPACE=/workspace

CMD ["python", "generate_ahs.py"]
