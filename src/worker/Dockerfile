FROM node:24-alpine

WORKDIR /app

COPY src/worker/package.json ./src/worker/package.json
RUN npm --prefix ./src/worker install --omit=dev

COPY src/shared ./src/shared
COPY src/worker/src ./src/worker/src

CMD ["node", "src/worker/src/index.js"]
