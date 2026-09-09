# A worker (or a serve) as its own machine. Running this in containers is what turns "any worker
# resumes any session" from a claim about processes into a claim about hosts: each of these has its
# own filesystem, its own hostname, and nothing of the session on disk. What they share is the
# Temporal cluster and one libSQL store, which is exactly what the README asks an operator to set up.

FROM oven/bun:1.3.14

# python3 and a compiler are here for one dependency: a tree-sitter grammar builds from source at
# install time. git is not incidental either: the snapshot packs a worker rebuilds a worktree from are git packs, so a
# host that has never seen the project needs it to materialize the tree.
RUN apt-get update \
  && apt-get install -y --no-install-recommends git ca-certificates procps curl \
       python3 make g++ \
  && rm -rf /var/lib/apt/lists/* \
  && git config --system user.email opencode@example.com \
  && git config --system user.name opencode \
  && git config --system init.defaultBranch main \
  && git config --system --add safe.directory '*'

WORKDIR /app

COPY package.json bun.lock bunfig.toml tsconfig.json* ./
COPY patches ./patches
COPY packages ./packages
RUN bun install --frozen-lockfile

ENV OPENCODE_SESSION_EXECUTION=temporal

# The worker by default. The serve role overrides this in compose; both build the same application
# context, so the only difference is whether an HTTP surface comes with it.
CMD ["bun", "run", "packages/server/src/worker.ts"]
