# Chain Proxy

Single-container dynamic chain proxy built on mihomo.

Flow:

```text
application -> local mixed port -> selected subscription node -> final SOCKS5 -> target
```

The container fetches the subscription itself, expands every subscription node into a
dedicated final-SOCKS5 chain proxy, and runs a concurrent watchdog against those
chain proxies. The selected proxy always exits through `FINAL_SOCKS5`.

The watchdog does not measure the bare subscription-node latency. It measures each
expanded chain proxy against `CHAIN_TEST_URL`, so the score includes:

```text
local container -> subscription node -> final SOCKS5 -> CHAIN_TEST_URL
```

That means a node is only considered healthy when it can reach and use the final
SOCKS5 successfully.

## Features

- Fetches a Clash-compatible subscription at runtime.
- Expands every subscription proxy into a complete chain proxy.
- Keeps the final exit fixed to your configured SOCKS5 proxy.
- Concurrently measures real end-to-end chain latency.
- Automatically switches to a lower-latency healthy chain.
- Keeps secrets out of the image; everything sensitive is runtime env.

## Quick Start

```bash
cp .env.example .env
# edit .env
docker compose build
docker compose up -d
```

Or run the published image directly:

```bash
docker run -d --name chain-proxy \
  --restart unless-stopped \
  -p 127.0.0.1:17898:17898 \
  -e SUB_URL='https://example.com/your-subscription?clash=3' \
  -e SOCKS_SERVER='192.0.2.10' \
  -e SOCKS_PORT='1080' \
  -e SOCKS_USERNAME='your-username' \
  -e SOCKS_PASSWORD='your-password' \
  -e CONTROLLER_SECRET='change-this-long-random-secret' \
  holtye/chain-proxy:latest
```

Test the proxy:

```bash
curl -x http://127.0.0.1:17898 -L -o /dev/null -sS \
  -w 'code=%{http_code} ttfb=%{time_starttransfer}s total=%{time_total}s\n' \
  https://claude.ai/

curl -x http://127.0.0.1:17898 https://api.ipify.org
```

The IP returned by `api.ipify.org` should be the final SOCKS5 exit IP.

## Environment

Required:

| Variable | Description |
| --- | --- |
| `SUB_URL` | Clash-compatible subscription URL. |
| `SOCKS_SERVER` | Final SOCKS5 server host/IP. |
| `SOCKS_PORT` | Final SOCKS5 server port. |
| `SOCKS_USERNAME` | Final SOCKS5 username. |
| `SOCKS_PASSWORD` | Final SOCKS5 password. |
| `CONTROLLER_SECRET` | Secret for the internal mihomo controller API. |

Optional:

| Variable | Default | Description |
| --- | ---: | --- |
| `MIXED_PORT` | `17898` | HTTP/SOCKS mixed proxy port inside the container. |
| `SUB_UPDATE_INTERVAL` | `3600` | Subscription refresh interval in seconds. |
| `CHAIN_TEST_URL` | `https://claude.ai/` | URL used for full-chain latency tests. |
| `WATCH_INTERVAL_SEC` | `60` | Watchdog interval in seconds. |
| `WATCH_TIMEOUT_MS` | `6000` | Per-candidate latency timeout. |
| `WATCH_CONCURRENCY` | `16` | Number of chain candidates tested concurrently. |
| `SWITCH_MARGIN_MS` | `150` | Required improvement before switching chains. |
| `SWITCH_STABLE_ROUNDS` | `2` | Required consecutive faster rounds before switching healthy chains. |
| `MIHOMO_LOG_LEVEL` | `info` | mihomo log level. |
| `SUB_FETCH_PROXY` | empty | Optional proxy used only to fetch the subscription. |

Compose-only host port variables:

| Variable | Default | Description |
| --- | ---: | --- |
| `HOST_MIXED_PORT` | `17899` | Host port mapped to `MIXED_PORT`. |
| `HOST_CONTROLLER_PORT` | `19091` | Host port mapped to mihomo controller `9090`. |

## GitHub Actions Publishing

This repository includes `.github/workflows/docker-publish.yml`.

Configure these GitHub repository secrets:

| Secret | Description |
| --- | --- |
| `DOCKERHUB_USERNAME` | Docker Hub username. |
| `DOCKERHUB_TOKEN` | Docker Hub access token or password. |

On pushes to `main`, CI publishes `holtye/chain-proxy:latest` and updates the
Docker Hub Overview from this README. Version tags like `v0.1.0` also publish
semver tags.
