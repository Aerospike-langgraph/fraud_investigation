FROM python:3.12.11-alpine3.22

ENV GRAPH_HOST_ADDRESS="asgraph-service"

RUN mkdir /backend
RUN apk add --no-cache \
    build-base \
    openssl-dev \
    linux-headers \
    zlib-dev \
    yaml-dev
COPY ./backend /backend
RUN mkdir -p /backend/scripts
COPY ./scripts/generate_user_data.py /backend/scripts/generate_user_data.py
WORKDIR /backend
RUN pip install -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--root-path", "/api", "--port", "4000", "--loop", "asyncio"]