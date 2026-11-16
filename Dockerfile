FROM oven/bun:1.3.14

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

COPY --chown=bun:bun . .
USER bun

EXPOSE 3000
CMD ["sh", "-c", "bun run db:migrate && exec bun src/main.ts"]
