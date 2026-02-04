FROM node:alpine3.22

ENV BACKEND_URL="http://asgraph-backend:4000"

RUN mkdir /generator
COPY ./generator /generator
WORKDIR /generator

RUN npm install

CMD [ "node", "index.js"]