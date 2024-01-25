FROM python:slim
WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y git gcc curl postgresql && \
    curl -sSL https://install.python-poetry.org | python - && \
    echo "export PATH=$PATH:/root/.local/bin" >> ~/.bashrc && \
    . ~/.bashrc && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# Run server when the container launches
CMD granian --no-ws --host 0.0.0.0 --port 8080 --interface asgi main:app
