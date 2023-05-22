FROM node:20-alpine3.16

COPY package.json package-lock.json /
RUN npm install
WORKDIR /app
CMD ["npx", "nodemon", "."]