# This Dockerfile should be built from the parent directory (tradr-backfiller)
# Build command: docker build -f tradr-dataplane/Dockerfile .

FROM node:20.11-alpine as build

RUN corepack enable

WORKDIR /app

# Copy atproto workspace root files
COPY ./atproto/package.json ./atproto/pnpm-lock.yaml ./atproto/pnpm-workspace.yaml ./atproto/
COPY ./atproto/tsconfig ./atproto/tsconfig

# Copy all required atproto packages (bsky and its dependencies)
COPY ./atproto/packages/bsky ./atproto/packages/bsky
COPY ./atproto/packages/api ./atproto/packages/api
COPY ./atproto/packages/common ./atproto/packages/common
COPY ./atproto/packages/common-web ./atproto/packages/common-web
COPY ./atproto/packages/crypto ./atproto/packages/crypto
COPY ./atproto/packages/did ./atproto/packages/did
COPY ./atproto/packages/identity ./atproto/packages/identity
COPY ./atproto/packages/sync ./atproto/packages/sync
COPY ./atproto/packages/syntax ./atproto/packages/syntax
COPY ./atproto/packages/lexicon ./atproto/packages/lexicon
COPY ./atproto/packages/repo ./atproto/packages/repo
COPY ./atproto/packages/xrpc ./atproto/packages/xrpc
COPY ./atproto/packages/xrpc-server ./atproto/packages/xrpc-server

# Copy internal packages
COPY ./atproto/packages/internal/fetch-node ./atproto/packages/internal/fetch-node
COPY ./atproto/packages/internal/fetch ./atproto/packages/internal/fetch
COPY ./atproto/packages/internal/pipe ./atproto/packages/internal/pipe
COPY ./atproto/packages/internal/xrpc-utils ./atproto/packages/internal/xrpc-utils

# Copy tradr-dataplane service
COPY ./tradr-dataplane ./tradr-dataplane

# Build atproto packages
WORKDIR /app/atproto
RUN pnpm install --frozen-lockfile
RUN pnpm build

# Clean up dev dependencies and install only production deps
RUN rm -rf node_modules
RUN pnpm install --prod --shamefully-hoist --frozen-lockfile --prefer-offline

# Set up NODE_PATH to find @atproto packages
ENV NODE_PATH=/app/atproto/node_modules:/app/atproto/packages

WORKDIR /app/tradr-dataplane

# Production stage
FROM node:20.11-alpine

# dumb-init is used to handle signals properly
RUN apk add --update dumb-init

# Avoid zombie processes, handle signal forwarding
ENTRYPOINT ["dumb-init", "--"]

WORKDIR /app

# Copy everything including node_modules with all dependencies
COPY --from=build /app /app

WORKDIR /app/tradr-dataplane

EXPOSE 3000
ENV BSKY_DATAPLANE_PORT=3000
ENV NODE_ENV=production

# https://github.com/nodejs/docker-node/blob/master/docs/BestPractices.md#non-root-user
USER node
CMD ["node", "--heapsnapshot-signal=SIGUSR2", "--enable-source-maps", "index.js"]

LABEL org.opencontainers.image.source=https://github.com/your-repo/tradr-backfiller
LABEL org.opencontainers.image.description="Tradr Data Plane Server"
LABEL org.opencontainers.image.licenses=MIT