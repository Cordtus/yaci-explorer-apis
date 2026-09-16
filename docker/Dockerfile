FROM postgrest/postgrest:v12.2.3 AS postgrest

# Build stage for Bun worker
FROM oven/bun:1.3-alpine AS builder

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

COPY . .

# Final stage combining both
FROM oven/bun:1.3-alpine

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache tini

# Copy PostgREST binary
COPY --from=postgrest /bin/postgrest /usr/local/bin/postgrest

# Copy Bun app with dependencies
COPY --from=builder /app /app

# Use tini as entrypoint for proper signal handling (-s for subreaper mode)
ENTRYPOINT ["/sbin/tini", "-s", "--"]

# Default to PostgREST (overridden by process config for worker process)
CMD ["/usr/local/bin/postgrest"]
