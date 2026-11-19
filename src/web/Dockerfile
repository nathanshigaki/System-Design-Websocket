FROM node:24-alpine

WORKDIR /app

COPY src/web/package.json ./src/web/package.json
RUN npm --prefix ./src/web install --omit=dev

COPY src/shared ./src/shared
COPY src/web/src ./src/web/src
COPY src/web/public ./src/web/public

CMD ["node", "src/web/src/index.js"]
