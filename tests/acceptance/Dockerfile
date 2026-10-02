FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# libfaketime сдвигает часы отдельных воркеров (A-CH-09); включается через LD_PRELOAD.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libfaketime \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dpkg -L libfaketime | grep '/libfaketime\.so\.1$')" /usr/local/lib/libfaketime.so.1

WORKDIR /app

COPY pyproject.toml README.md LICENSE ./
COPY src ./src

RUN python -m pip install --no-cache-dir -e ".[asyncpg,flexiq]" "aiohttp>=3.12"

# Тесты копируются последними: их правка не пересобирает слой с зависимостями.
COPY tests ./tests

CMD ["python", "-m", "tests.acceptance.app.worker", "--help"]
