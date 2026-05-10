# Forerunner backend container.
#
# Layered for build cache efficiency: requirements first (rarely change),
# code last (changes every commit). This keeps `docker build` fast on
# repeat builds.
#
# Runs as non-root for security. Listens on the port Railway assigns
# via $PORT (defaults to 8080 locally for parity with `python main.py`).

FROM python:3.12-slim

# Avoid .pyc clutter and force unbuffered stdout so Railway logs are real-time
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# System deps. Slim image lacks build tooling — only install what we need.
# httpx + pydantic + web3 all have wheel distributions on PyPI for Python 3.12,
# so we don't need gcc. If a future dep needs compilation, add `build-essential`.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install Python deps first — this layer is cached unless requirements.txt changes
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app code last
COPY . .

# Create and switch to non-root user
RUN useradd --create-home --uid 1000 forerunner && \
    chown -R forerunner:forerunner /app
USER forerunner

# Railway sets $PORT dynamically; honor it. Local default 8080 keeps parity
# with the docs and your existing .env.
ENV API_HOST=0.0.0.0 \
    API_PORT=8080

# Healthcheck endpoint. Railway pings this; if it fails the container restarts.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{__import__(\"os\").environ.get(\"API_PORT\", \"8080\")}/health', timeout=3).read()" || exit 1

# Railway expects the process to honor $PORT. Our config.py already reads it
# (via API_PORT env var), but we also need to bind 0.0.0.0 — which we set above.
CMD ["python", "main.py"]
