FROM metacubex/mihomo@sha256:23e7666401c91e2e293e01fb2f842b0a75530821c29b8ff2ed79409cd51fc74f AS mihomo

FROM python:3.12-alpine

RUN pip install --no-cache-dir PyYAML==6.0.3

COPY --from=mihomo /mihomo /usr/local/bin/mihomo
COPY app/chain_proxy.py /app/chain_proxy.py

ENTRYPOINT ["python", "/app/chain_proxy.py"]
