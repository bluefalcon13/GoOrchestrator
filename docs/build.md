# Build, Packaging & Deployment Guide

This guide covers building the `GoOrchestrator` binary on host systems, static compilation, packaging into minimalist containers, and deploying behind Cloudflare Tunnels.

---

## 1. Build Targets (`Makefile`)

The root [`Makefile`](../Makefile) standardizes builds, tests, and cleanup into `./build`:

| Target | Command | Description |
| :--- | :--- | :--- |
| `make build` | `go build -trimpath -ldflags "-s -w"` | Compiles standard binary into `./build/orchestrator` using host Go toolchain. |
| `make build-static` | `CGO_ENABLED=0 go build ...` | Compiles a statically linked, CGO-disabled binary into `./build/orchestrator`. |
| `make docker-build` | `docker build -f builder/Dockerfile` | Builds the ultra-minimal `FROM scratch` Docker container (~13MB). |
| `make test` | `go test -v -count=1 ./...` | Runs all unit and integration tests across all packages. |
| `make test-coverage` | `go test -coverprofile=...` | Runs test suite and outputs coverage metrics to `./build/coverage.out`. |
| `make clean` | `rm -rf ./build` | Removes `./build` directory and all compiled artifacts. |
| `make help` | `make help` | Prints the list of available targets. |

---

## 2. Compilation Flags Explained

When compiling production Go binaries, specific flags optimize security, size, and portability:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o ./build/orchestrator \
    ./main.go
```

- **`CGO_ENABLED=0`**: Disables the cgo foreign-function interface. The Go compiler outputs pure Go machine code without dynamic dependencies on `libc.so` or `musl`.
- **`-trimpath`**: Removes host file system paths from stack traces, panics, and binary metadata, preventing leaking developer workspace structure.
- **`-ldflags="-s -w"`**:
  - `-s`: Strips symbol table information.
  - `-w`: Disables DWARF debugging generation.
  - Combined, these flags reduce binary size by approximately 30–40% without impacting runtime execution or panic stack traces.
- **`-extldflags '-static'`**: Instructs the internal linker to link any low-level primitives purely statically.

You can verify the static nature of the binary:
```bash
file ./build/orchestrator
# Output: ... ELF 64-bit LSB executable, x86-64, version 1 (SYSV), statically linked, stripped

ldd ./build/orchestrator
# Output: not a dynamic executable
```

---

## 3. Minimalist Containerization (`builder/Dockerfile`)

Because `GoOrchestrator` is a statically linked executable with zero C-dependencies, it runs directly from Docker's empty base image: `FROM scratch`.

### Dockerfile Breakdown

```dockerfile
# Stage 1: Build binary and prepare unprivileged user
FROM golang:1.22-alpine AS builder
WORKDIR /src
RUN echo "orchestrator:x:10001:10001:GoOrchestrator User:/nonexistent:/bin/false" > /etc/passwd.scratch
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/orchestrator \
    ./main.go

# Stage 2: Minimalist Scratch Container (~13MB total)
FROM scratch
COPY --from=builder /etc/passwd.scratch /etc/passwd
COPY --from=builder /bin/orchestrator /orchestrator

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["/orchestrator"]
CMD ["-port", "8080"]
```

### Why Omit CA Certificates?
In isolated container deployments where the orchestrator runs behind a **Cloudflare Tunnel** (`cloudflared`) and accesses LLMs on an internal Docker network:
1. Inbound public HTTPS traffic is terminated at Cloudflare's edge and delivered over the tunnel to the orchestrator as plain HTTP (`:8080`).
2. Outbound traffic to local model backends (vLLM, Ollama) occurs over plain HTTP (`http://vllm:8000`), which bypasses Go's TLS certificate verification code path (`crypto/x509`).
3. As a result, CA certificates are unnecessary, reducing container weight and removing any certificate rotation overhead.

---

## 4. Docker Compose & Cloudflare Tunnel Deployment

The [`builder/example_docker_compose.yaml`](../builder/example_docker_compose.yaml) shows how to deploy `GoOrchestrator` securely:

```yaml
version: '3.8'

services:
  orchestrator:
    build:
      context: ..
      dockerfile: builder/Dockerfile
    image: go-orchestrator:latest
    container_name: go-orchestrator
    networks:
      - internal-mesh
    command:
      - "-port"
      - "8080"
      - "-mode"
      - "http"
      - "-lrm-url"
      - "http://lrm-vllm:8000"
      - "-worker-url"
      - "http://worker-vllm:8001"
    restart: unless-stopped
    depends_on:
      - lrm-vllm
      - worker-vllm

  cloudflared:
    image: cloudflare/cloudflared:latest
    container_name: cloudflared-tunnel
    networks:
      - internal-mesh
    restart: unless-stopped
    command: tunnel --no-autoupdate run --token ${CLOUDFLARE_TUNNEL_TOKEN}

  lrm-vllm:
    image: vllm/vllm-openai:latest
    networks:
      - internal-mesh
    command: ["--model", "Qwen/Qwen2.5-32B-Instruct", "--port", "8000"]
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]

  worker-vllm:
    image: vllm/vllm-openai:latest
    networks:
      - internal-mesh
    command: ["--model", "Qwen/Qwen2.5-Coder-7B-Instruct", "--port", "8001"]
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]

networks:
  internal-mesh:
    driver: bridge
```

### Starting the Stack
```bash
# Export your Cloudflare Tunnel token
export CLOUDFLARE_TUNNEL_TOKEN="your-tunnel-token-here"

# Start the stack from builder/
cd builder
docker compose -f example_docker_compose.yaml up -d
```

