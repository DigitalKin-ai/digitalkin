FROM python:3.13-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=off \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

# Install SDK from source
COPY pyproject.toml ./
COPY src/ ./src/

# Install SDK + its dependencies
RUN uv venv && uv pip install -e ".[performance]"

# Copy bench module
COPY examples/bench_module/ ./bench_module/

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 50055

CMD ["python", "-m", "bench_module.server"]
