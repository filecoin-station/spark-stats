# syntax = docker/dockerfile:1

# Adjust NODE_VERSION as desired
FROM node:22.14.0-slim AS base

LABEL fly_launch_runtime="nodejs"

# Node.js app lives here
WORKDIR /app

# Set production environment
ENV NODE_ENV=production
ENV SENTRY_ENVIRONMENT=production

#######################################################################
# Throw-away build stage to reduce size of final image
FROM base AS build

# Install packages needed to build node modules
RUN apt-get update -qq && \
  apt-get install -y build-essential pkg-config python-is-python3

# Install node modules
# NPM will not install any package listed in "devDependencies" when NODE_ENV is set to "production",
# to install all modules: "npm install --production=false".
# Ref: https://docs.npmjs.com/cli/v9/commands/npm-install#description
COPY --link package-lock.json package.json ./

# We cannot use a wildcard until `COPY --parents` is stabilised
# See https://docs.docker.com/reference/dockerfile/#copy---parents
COPY --link db/package.json ./db/
COPY --link stats/package.json ./stats/
COPY --link observer/package.json ./observer/

RUN npm ci --workspaces

# Copy application code
COPY --link . .

#######################################################################
# Final stage for app image
FROM base

# Copy built application
COPY --from=build /app /app

# Set to `stats` or `observer`
# This argument controls the value used by npm to choose which workspace (subdir) to start
ENV NPM_CONFIG_WORKSPACE=""

CMD [ "npm", "start" ]
