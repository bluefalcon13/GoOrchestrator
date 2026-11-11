# ------------------------------------------------------------------------------
# Stage 1: Build static executable
# ------------------------------------------------------------------------------
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Create an unprivileged user definition for the scratch container (no root runtime)
RUN echo "orchestrator:x:10001:10001:GoOrchestrator User:/nonexistent:/bin/false" > /etc/passwd.scratch

# Cache dependencies
COPY go.mod ./
# COPY go.sum ./
RUN go mod download

# Copy source tree
COPY . .

# Pure static compilation without symbols or DWARF overhead
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/orchestrator \
    ./main.go

# ------------------------------------------------------------------------------
# Stage 2: Pure Minimalist Scratch Runtime (~13MB total)
# ------------------------------------------------------------------------------
FROM scratch

# Non-root user definition
COPY --from=builder /etc/passwd.scratch /etc/passwd

# Static binary
COPY --from=builder /bin/orchestrator /orchestrator

# Drop root privileges immediately
USER 10001:10001

# Expose Conversational Intake API port
EXPOSE 8080

ENTRYPOINT ["/orchestrator"]
CMD ["-port", "8080"]

