# NexusFlow production image (multi-stage).
#
# The same image serves every NexusFlow role; the role is selected at
# runtime with NEXUSFLOW_ROLE (api | scheduler | worker | outbox-relay).
# Run `npm run migrate:prod` (or `node dist/migrate.js`) once per deploy,
# from a single process, before starting the roles.
#
#   docker build -t nexusflow .
#   docker run --rm -e DATABASE_URL='...' nexusflow node dist/migrate.js
#   docker run -d -p 3000:3000 -e DATABASE_URL='...' -e NEXUSFLOW_ROLE=api nexusflow

FROM node:22-slim AS build

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM node:22-slim AS production

ENV NODE_ENV=production

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Compiled app + migration entry (dist/migrate.js) + packaged migration SQL.
COPY --from=build /app/dist ./dist

USER node

EXPOSE 3000

CMD ["node", "dist/main.js"]
