FROM python:3.10-slim as python-base

ARG SCRIPT_DIR

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.14 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup" \
    VENV_PATH="/opt/pysetup/.venv"

ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

FROM python-base as builder-base
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        build-essential

RUN curl -sSL https://install.python-poetry.org | python3 -

WORKDIR $PYSETUP_PATH
COPY ${SCRIPT_DIR}/poetry.lock ${SCRIPT_DIR}/pyproject.toml ./

ENV PATH="${PATH}:/root/.poetry/bin"

RUN poetry install --no-dev

FROM python-base as development

RUN apt-get update && apt-get upgrade -y curl \
      awscli

WORKDIR $PYSETUP_PATH

COPY --from=builder-base $POETRY_HOME $POETRY_HOME
COPY --from=builder-base $PYSETUP_PATH $PYSETUP_PATH
ENV PATH="${PATH}:/root/.poetry/bin"

ARG NOTIFY_WEBHOOK_URL
ENV NOTIFY_WEBHOOK_URL=${NOTIFY_WEBHOOK_URL}
ENV EPID_INGESTION_ENV "XXXXXXXXXXX"
ENV EPID_INGESTION_SOURCE_ID "XXXXXXXXXXX"

WORKDIR /app

RUN mkdir -p templates

COPY ${SCRIPT_DIR}/* ./

RUN poetry install --no-dev

CMD ./run.sh