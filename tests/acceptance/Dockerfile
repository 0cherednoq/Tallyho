FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY tests ./tests

RUN python -m pip install --no-cache-dir -e ".[asyncpg,flexiq]" "aiohttp>=3.12"

CMD ["python", "-m", "tests.acceptance.app.worker", "--help"]
