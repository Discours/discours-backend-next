FROM python:alpine
WORKDIR /app
COPY . /app

RUN apk update && apk add --no-cache build-base icu-data-full curl python3-dev musl-dev postgresql-dev postgresql-client
RUN curl -sSL https://install.python-poetry.org | python
ENV PATH="${PATH}:/root/.local/bin"
RUN poetry config virtualenvs.create false && poetry install --only main

EXPOSE 8000

CMD ["python", "server.py"]
