FROM node:24-alpine

WORKDIR /app

COPY src/gateway/package.json ./src/gateway/package.json
RUN npm --prefix ./src/gateway install --omit=dev

COPY src/shared ./src/shared
COPY src/gateway/src ./src/gateway/src

CMD ["node", "src/gateway/src/index.js"]
