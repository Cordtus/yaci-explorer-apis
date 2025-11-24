FROM postgrest/postgrest:v12.2.3 AS postgrest

# Build stage for Node.js worker
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

COPY . .

# Final stage combining both
FROM node:20-alpine

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache tini

# Copy PostgREST binary
COPY --from=postgrest /bin/postgrest /usr/local/bin/postgrest

# Copy Node.js app with dependencies
COPY --from=builder /app /app

# Use tini as entrypoint for proper signal handling
ENTRYPOINT ["/sbin/tini", "--"]

# Default to PostgREST (overridden by fly.toml for worker process)
CMD ["/usr/local/bin/postgrest"]
