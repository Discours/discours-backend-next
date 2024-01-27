FROM python:slim
WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install -y git gcc curl postgresql && \
    curl -sSL https://install.python-poetry.org | python - && \
    echo "export PATH=$PATH:/root/.local/bin" >> ~/.bashrc && \
    . ~/.bashrc && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

EXPOSE 8000

# Run server when the container launches
CMD python server.py
