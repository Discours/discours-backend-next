## Техстек


- sqlalchemy
- redis
- ariadne
- starlette
- granian

# Локальная разработка

Подготовьте зависимости

osx:
```
brew install redis nginx postgres
brew services start redis
```

debian/ubuntu:
```
apt install redis nginx
```

Затем запустите postgres, redis и наш API-сервер:

```shell
mkdir .venv
python3.12 -m venv .venv
poetry env use .venv/bin/python3.12
poetry update
poetry run main.py
```
## Подключенные сервисы

Для межсерверной коммуникации используются отдельные логики, папка `services/*` содержит адаптеры для использования базы данных, `redis`, кеширование и клиенты для запросов GraphQL.

### auth.py

Задайте переменную окружения `WEBHOOK_SECRET` чтобы принимать запросы по адресу `/new-author` от [сервера авторизации](https://dev.discours.io/devstack/authorizer). Событие ожидается при создании нового пользователя. Для авторизованных запросов и мутаций фронтенд добавляет к запросу токен авторизации в заголовок `Authorization`.

### viewed.py

Задайте переменные окружения `GOOGLE_KEYFILE_PATH` и `GOOGLE_PROPERTY_ID` для получения данных из [Google Analytics](https://developers.google.com/analytics?hl=ru).

### search.py

Позволяет получать результаты пользовательских поисковых запросов в кешируемом виде от ElasticSearch с оценкой `score`, объединенные с запросами к базе данных, запрашиваем через GraphQL API `load_shouts_search`. Требует установка `ELASTIC_HOST`, `ELASTIC_PORT`, `ELASTIC_USER` и `ELASTIC_PASSWORD`.

### notify.py

Отправка уведомлений по Redis PubSub каналам, согласно структуре данных, за которую отвечает [сервис уведомлений](https://dev.discours.io/discours.io/notifier)

###  unread.py

Счетчик непрочитанных сообщений получается через Redis-запрос к данным [сервиса сообщений](https://dev.discours.io/discours.io/inbox).

### following.py

Внутренний сервис, обеспечивающий асинхронный доступ к хранилищу подписчиков на комментарии, топики и авторы в оперативной памяти.
