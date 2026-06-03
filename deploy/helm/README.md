# NexusDL Helm Chart

Distributed, cluster-aware multi-protocol download system.

## Quick Start

```bash
# Add the repo (after first release)
helm repo add nexus-dl https://nexus-dl.github.io/helm-charts
helm repo update

# Install with auto-generated auth token
helm install nexus-dl nexus-dl/nexus-dl --namespace nexus-dl --create-namespace

# Install with a known auth token
helm install nexus-dl nexus-dl/nexus-dl \
  --set authToken=$(openssl rand -hex 32) \
  --namespace nexus-dl --create-namespace
```

To retrieve the auth token (if not provided):

```bash
kubectl get secret -n nexus-dl nexus-dl -o jsonpath='{.data.auth-token}' | base64 -d
```

## Cluster Mode (3 nodes)

```bash
helm install nexus-dl-1 nexus-dl/nexus-dl \
  --set replicaCount=1 \
  --set nameOverride=nexus-dl-1

helm install nexus-dl-2 nexus-dl/nexus-dl \
  --set replicaCount=1 \
  --set nameOverride=nexus-dl-2 \
  --set cluster.joinPeers={nexus-dl-1:50052}

helm install nexus-dl-3 nexus-dl/nexus-dl \
  --set replicaCount=1 \
  --set nameOverride=nexus-dl-3 \
  --set cluster.joinPeers={nexus-dl-1:50052}
```

Each node must have a different `nameOverride` to be discoverable as a
distinct peer.

## Configuration

| Value | Description | Default |
| --- | --- | --- |
| `replicaCount` | Number of pod replicas per release | `1` |
| `image.repository` | Container image | `ghcr.io/nexus-dl/nexus-dl` |
| `image.tag` | Image tag (defaults to `appVersion`) | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `authToken` | API auth token. Random if empty. | `""` |
| `rateLimit` | HTTP requests/sec per IP, 0 = disable | `100` |
| `logLevel` | `debug`, `info`, `warn`, `error` | `info` |
| `cluster.joinPeers` | Seed peers (host:port UDP) | `[]` |
| `cluster.grpcPort` | gRPC port | `50051` |
| `cluster.discoveryPort` | UDP discovery port | `50052` |
| `persistence.enabled` | Enable PVC | `true` |
| `persistence.size` | PVC size | `50Gi` |
| `persistence.storageClass` | Storage class name | `""` |
| `service.type` | K8s service type | `ClusterIP` |
| `service.api.port` | API port | `8080` |
| `ingress.enabled` | Enable Ingress | `false` |
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |
| `resources.limits` | Resource limits | `cpu=2, mem=1Gi` |
| `resources.requests` | Resource requests | `cpu=100m, mem=128Mi` |
| `podSecurityContext` | Pod-level security | non-root, read-only fs |
| `securityContext` | Container-level security | `readOnlyRootFilesystem: true` |

## Exposed Endpoints

| Service port | Container port | Protocol | Purpose |
| --- | --- | --- | --- |
| `service.api.port` | `8080` | TCP | REST API, JSON-RPC, XML-RPC, WebSocket |
| `service.grpc.port` | `50051` | TCP | Inter-node gRPC |
| `service.discovery.port` | `50052` | UDP | Peer discovery |

## Upgrading

```bash
helm repo update
helm upgrade nexus-dl nexus-dl/nexus-dl
```

To bump the image tag without re-rendering the chart:

```bash
helm upgrade nexus-dl nexus-dl/nexus-dl --reuse-values --set image.tag=0.2.0
```

## Uninstalling

```bash
helm uninstall nexus-dl --namespace nexus-dl
# PVC is not deleted by default; remove it manually if you want a clean slate:
kubectl delete pvc -n nexus-dl nexus-dl-data
```

## Security Notes

- The chart runs as non-root user (uid `65532`).
- The root filesystem is read-only; the data directory is mounted from a PVC.
- All Linux capabilities are dropped.
- The cluster gRPC and UDP discovery ports should be on a private network
  in production. For multi-node cluster mode, consider using
  `NetworkPolicy` to restrict these to other `nexus-dl` pods only.

## Linting

```bash
helm lint deploy/helm
helm template nexus-dl deploy/helm | less
```

## Testing (helm-unittest plugin)

```bash
helm plugin install https://github.com/helm-unittest/helm-unittest
helm unittest deploy/helm
```
