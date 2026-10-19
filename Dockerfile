FROM python:3.13-slim AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir --prefix=/install .

FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 GX_ANALYTICS_ENABLED=False API_KEYS_FILE=/app/config/api_keys.yaml
RUN useradd --create-home --uid 10001 app
COPY --from=build /install /usr/local
WORKDIR /app
COPY config ./config
RUN mkdir -p /data && chown app /app /data
USER app
EXPOSE 8000
CMD ["telemetry", "serve", "--data", "/data", "--host", "0.0.0.0", "--port", "8000"]
