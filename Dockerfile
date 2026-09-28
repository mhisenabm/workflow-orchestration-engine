FROM python:3.12-slim AS source

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY apps ./apps
COPY packages ./packages
COPY infrastructure ./infrastructure
COPY tests ./tests
COPY examples ./examples
COPY docs ./docs

FROM source AS runtime
RUN python -m pip install --upgrade pip && python -m pip install .
RUN useradd --create-home --uid 10001 woe && chown -R woe:woe /app
USER woe

FROM source AS test
RUN python -m pip install --upgrade pip && python -m pip install -e '.[dev]'
RUN useradd --create-home --uid 10001 woe && chown -R woe:woe /app
USER woe
