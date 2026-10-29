FROM node:24-alpine

WORKDIR /app

ENV NODE_ENV=production

COPY package.json ./

RUN npm install --omit=dev

COPY . .

RUN chown -R node:node /app

USER node

EXPOSE 3000 4001 9101

CMD ["node", "src/server.js"]