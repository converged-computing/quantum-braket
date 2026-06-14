FROM python:3.11-slim

LABEL org.opencontainers.image.source="https://github.com/converged-computing/quantum-braket"
LABEL org.opencontainers.image.description="QAOA transpiler"

WORKDIR /app
COPY transpile.py .

ENV P_LAYERS=1
ENV WORKSPACE=/workspace

CMD ["python", "transpile.py"]
