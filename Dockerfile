# syntax=docker/dockerfile:1.7
#
# Image shared by every processor service in the docker-compose.yml emitted
# by `medallion/generate_compose.py`. The compose `command:` per service
# selects which run module to invoke; this image only needs to provide:
#
#   - the medallion runtime deps,
#   - the medallion package on PYTHONPATH,
#   - the user-processor package at MEDALLION_ROOT (/app/src by default).
#
# To swap your own processors in without rebuilding, mount them over the
# baked-in copy:
#
#     services:
#       acme-products:
#         volumes:
#           - ./src:/app/src
#
# --------------------------------------------------------------------------- #
# Builder: resolve runtime deps into /app/.venv via Poetry.
# --------------------------------------------------------------------------- #
FROM python:3.13-slim AS builder

ENV POETRY_VERSION=2.1.4 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

WORKDIR /app
COPY pyproject.toml poetry.lock README.md ./

# --no-root: don't try to install the local medallion package — we COPY it
# into the runtime stage directly. uvicorn lives in the dev group upstream
# but is needed at runtime by medallion/run/extractor.py, so install it
# explicitly into the venv.
RUN poetry install --no-root --only main \
 && /app/.venv/bin/pip install --no-cache-dir "uvicorn>=0.47,<0.48"


# --------------------------------------------------------------------------- #
# Runtime: slim image carrying the venv + sources only.
# --------------------------------------------------------------------------- #
FROM python:3.13-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY medallion/ /app/medallion/
COPY example/nemweb/ /app/src/

RUN mkdir -p /app/data

# No CMD/ENTRYPOINT — the generated docker-compose.yml supplies the command
# per service (extractor / transformer / store).
