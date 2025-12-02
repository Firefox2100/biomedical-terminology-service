FROM python:3.11-slim AS cpu

LABEL org.opencontainers.image.title="BioMedical Terminology Service"
LABEL org.opencontainers.image.description="A FastAPI service for using with biomedical terminologies, such as ontologies or vocabularies."
LABEL org.opencontainers.image.authors="wangyunze16@gmail.com"
LABEL org.opencontainers.image.url="https://github.com/Firefox2100/biomedical-terminology-service"
LABEL org.opencontainers.image.source="https://github.com/Firefox2100/biomedical-terminology-service"
LABEL org.opencontainers.image.vendor="uk.co.firefox2100"
LABEL org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1
ENV BTS_ENV_FILE="/app/conf/.env"
ENV BTS_DATA_DIR="/app/data"

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system appgroup && \
    useradd --system --no-create-home --gid appgroup appuser

WORKDIR /app
COPY ./src/bioterms /app/src/bioterms
COPY ./conf /app/conf
COPY ./pyproject.toml /app/pyproject.toml
COPY ./example.env /app/conf/.env
COPY ./LICENSE /app/LICENSE
COPY ./README.md /app/README.md
COPY ./scripts/entrypoint.sh /app/entrypoint.sh

ARG TORCH_VERSION=2.8.0
RUN pip install --upgrade pip && \
    pip install "torch==${TORCH_VERSION}" --index-url https://download.pytorch.org/whl/cpu && \
    pip install .[all] && \
    chown -R appuser:appgroup /app

USER appuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl --fail http://localhost:5000/health || exit 1

VOLUME ["/app/conf", "/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--host", "0.0.0.0", "--port", "5000", "--log-config", "/app/conf/uvicorn-log.config.yaml"]

FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04 AS gpu

LABEL org.opencontainers.image.title="BioMedical Terminology Service"
LABEL org.opencontainers.image.description="A FastAPI service for using with biomedical terminologies, such as ontologies or vocabularies."
LABEL org.opencontainers.image.authors="wangyunze16@gmail.com"
LABEL org.opencontainers.image.url="https://github.com/Firefox2100/biomedical-terminology-service"
LABEL org.opencontainers.image.source="https://github.com/Firefox2100/biomedical-terminology-service"
LABEL org.opencontainers.image.vendor="uk.co.firefox2100"
LABEL org.opencontainers.image.licenses="MIT"

ENV PYTHONUNBUFFERED=1
ENV BTS_ENV_FILE="/app/conf/.env"
ENV BTS_DATA_DIR="/app/data"

RUN apt-get update && apt-get install -y --no-install-recommends \
    bash ca-certificates curl python3-pip python3.11 python3.11-venv && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system appgroup && \
    useradd --system --no-create-home --gid appgroup appuser && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --set python3 /usr/bin/python3.11 && \
    update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    update-alternatives --set python /usr/bin/python3.11 && \
    update-alternatives --install /usr/bin/pip pip /usr/bin/pip3 1 && \
    update-alternatives --set pip /usr/bin/pip3

WORKDIR /app
COPY ./src/bioterms /app/src/bioterms
COPY ./conf /app/conf
COPY ./pyproject.toml /app/pyproject.toml
COPY ./example.env /app/conf/.env
COPY ./LICENSE /app/LICENSE
COPY ./README.md /app/README.md
COPY ./scripts/entrypoint.sh /app/entrypoint.sh

ARG TORCH_VERSION=2.8.0
RUN pip install --upgrade pip && \
    pip install "torch==${TORCH_VERSION}" --index-url https://download.pytorch.org/whl/cu126 && \
    pip install .[all] && \
    chown -R appuser:appgroup /app

USER appuser

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl --fail http://localhost:5000/health || exit 1

VOLUME ["/app/conf", "/app/data"]

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["--host", "0.0.0.0", "--port", "5000", "--log-config", "/app/conf/uvicorn-log.config.yaml"]
