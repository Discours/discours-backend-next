FROM python:alpine3.18
WORKDIR /app
COPY . /app

RUN apk update && apk add --no-cache git gcc curl postgresql-client

RUN curl -sSL https://install.python-poetry.org | python - && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# Expose port 8000
EXPOSE 8000

# Run server when the container launches
CMD ["python", "server.py"]
