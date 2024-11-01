# discoursio-api

## Техстек

- sqlalchemy
- redis
- ariadne
- starlette
- granian

## Локальная разработка

Запустите API-сервер с ключом `dev`:

```shell
mkdir .venv
python3.12 -m venv .venv
poetry env use .venv/bin/python3.12
poetry update
poetry run server.py dev
```

### Полезные команды

```shell
poetry run ruff check . --fix --select I # линтер и сортировка импортов
poetry run ruff format . --line-length=120 # форматирование кода
```

