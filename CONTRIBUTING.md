# Contributing to NexusDL

Thank you for your interest in improving NexusDL! This guide explains how to
file issues, propose changes, and submit pull requests.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you are expected to uphold it.

## Filing Issues

- **Bugs**: use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.yml).
- **Feature requests**: use the [feature request template](.github/ISSUE_TEMPLATE/feature_request.yml).
- **Security issues**: see [SECURITY.md](SECURITY.md) — do **not** file a
  public issue.

## Development Setup

Requirements:

- Go **1.22+**
- `git`, `make` (or `go` directly on Windows)
- For cluster testing: `docker` and `docker compose`

```bash
git clone https://github.com/nexus-dl/nexus-dl.git
cd nexus-dl
go mod download
make build          # produces ./bin/nexus-dl
make test           # runs all unit tests
```

Useful targets (see [Makefile](Makefile)):

| Target | Description |
| --- | --- |
| `make build` | Build binary into `bin/nexus-dl` (version-injected) |
| `make run` | Run `serve` against the local `config.yaml` |
| `make test` | Run `go test ./...` |
| `make test-race` | Run with the race detector (requires CGO) |
| `make cover` | Produce `coverage.html` |
| `make vet` | `go vet ./...` |
| `make fmt` | `gofmt -w .` |
| `make lint` | `go vet` + `gofmt` + `goimports` check |
| `make tidy` | `go mod tidy` |
| `make docker` | Build the Docker image locally |
| `make compose-up` / `make compose-down` | Bring up / tear down the dev cluster |
| `make clean` | Remove build artifacts |

## Coding Style

- Format with `gofmt` and `goimports` (CI enforces both).
- Run `go vet ./...` locally before pushing.
- Keep exported symbols documented with Go doc comments.
- Use the existing package layout (see `Project Layout` in the README).
- Prefer composition over inheritance; small interfaces over large ones.
- Add or update unit tests for any behaviour change.

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short summary>

<body explaining motivation, approach, and trade-offs>

<footer with "BREAKING CHANGE:" notes and issue references>
```

Common types: `feat`, `fix`, `refactor`, `perf`, `test`, `docs`, `chore`,
`build`, `ci`, `revert`.

Examples:

```
feat(api): add /api/v1 prefix and deprecate unversioned routes

The v1 path is now the canonical location. The old paths continue to
work for one minor version and emit Deprecation headers.

Closes #123
```

## Pull Requests

1. Fork the repository and create a branch off `main`.
2. Make focused commits with clear messages.
3. Run `make lint test` before pushing — CI will reject failures.
4. Add a short PR description (the [template](.github/PULL_REQUEST_TEMPLATE.md)
   guides you).
5. Link the issue(s) the PR addresses.
6. Expect a review within a few business days. Be ready to iterate.

CI will automatically run lint, tests on Linux / macOS / Windows, build for
five platforms, and build the Docker image. See
[`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Release Process

1. Bump `CHANGELOG.md` under a new `## [Unreleased]` → `## [X.Y.Z] - DATE`
   section.
2. Tag the commit: `git tag -s vX.Y.Z`.
3. Push the tag: `git push origin vX.Y.Z`.
4. The release workflow builds binaries for all platforms, builds multi-arch
   Docker images, and creates a GitHub Release.
5. A maintainer publishes to the package registries if applicable.

## Adding a New Protocol Downloader

If you want to add support for a new protocol (e.g. SMB):

1. Create `internal/downloader/<protocol>.go` implementing the
   `Downloader` interface.
2. Register the scheme(s) in the protocol switch in `manager.go`.
3. Add unit tests in `<protocol>_test.go`.
4. Update the README feature list and the OpenAPI `Task.protocol` enum.
5. If the protocol needs its own config section, extend `pkg/config`.

## Adding a New Aria2-Compatible RPC Method

1. Add the method to `internal/api/jsonrpc.go` (and `xmlrpc.go` if it must
   be available over XML-RPC).
2. Cover it with a unit test in `internal/api/`.
3. Document it under "Aria2-compatible RPC" in the README.

## Reporting a Vulnerability

See [SECURITY.md](SECURITY.md). Please do not file security issues in
public.
