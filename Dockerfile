FROM python:alpine3.18
WORKDIR /app
COPY . /app

RUN apk update && apk add --no-cache git gcc curl postgresql-client
RUN curl -sSL https://install.python-poetry.org | python
ENV PATH="${PATH}:/root/.local/bin"
RUN poetry config virtualenvs.create false && poetry install --no-dev

EXPOSE 8000

CMD ["python", "server.py"]
