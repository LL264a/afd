# Deployment

This directory contains production deployment manifests for NexusDL.

| Path | Purpose |
| --- | --- |
| `systemd/nexus-dl.service` | systemd unit (Linux) |
| `nginx/nexus-dl.conf` | nginx reverse proxy with TLS + rate limiting |
| `prometheus/alerts.yml` | Prometheus alert rules |
| `prometheus/scrape.yml` | Prometheus scrape config example |
| `grafana/nexus-dl.json` | Grafana dashboard |
| `helm/` | Kubernetes Helm chart |

## Docker / docker-compose

See the top-level `docker-compose.yml` for a 3-node local cluster:

```bash
docker compose up -d
```

The Docker image is built by the top-level `Dockerfile`.

## Quick Decision Tree

| Scenario | Use |
| --- | --- |
| Local dev / single node | `docker compose up -d` |
| Linux VM / bare metal | `systemd/nexus-dl.service` |
| Kubernetes | `helm/` chart |
| Public internet exposure | `nginx/nexus-dl.conf` in front |

## Production Checklist

- [ ] Set `NEXUS_API_AUTH_TOKEN` to ≥ 32 random bytes
- [ ] Put a reverse proxy with TLS in front of the API port
- [ ] Restrict gRPC and UDP discovery ports to a private network
- [ ] Mount a quota-limited filesystem for the data directory
- [ ] Enable Prometheus scraping and the `alerts.yml` rules
- [ ] Configure log shipping (Loki, Elasticsearch, ...)
- [ ] Set up persistent backups of the `data/tasks/` directory
- [ ] Pin the image / binary version (do not auto-upgrade)
