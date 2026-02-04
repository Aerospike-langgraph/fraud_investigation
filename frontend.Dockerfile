FROM node:alpine3.22

ENV BACKEND_URL="http://asgraph-backend:4000"
ENV GENERATOR_URL="http://asgraph-generator:4001"
ENV BASE_URL="http://localhost:8080/api"

RUN mkdir /frontend
COPY ./frontend /frontend
WORKDIR /frontend

RUN npm install

CMD [ "sh", "-c", "wget --post-data='' http://asgraph-backend:4000/bulk-load-gremlin ; npm run deploy"]