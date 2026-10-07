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

RUN set -eu; \
    for attempt in 1 2 3; do \
        if python -m pip install --no-cache-dir --retries 10 --timeout 60 \
            -e ".[asyncpg,flexiq]" "aiohttp>=3.12"; then \
            exit 0; \
        fi; \
        if [ "$attempt" -eq 3 ]; then \
            exit 1; \
        fi; \
        sleep $((attempt * 5)); \
    done

# Тесты копируются последними: их правка не пересобирает слой с зависимостями.
COPY tests ./tests

CMD ["python", "-m", "tests.acceptance.app.worker", "--help"]
