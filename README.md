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
poetry granian --no-ws --host 0.0.0.0 --port 8080 --interface asgi main:app
```
## Подключенные сервисы

Для межсерверной коммуникации используется разные механики, похожим образом это устроено в других сервисах нашей инфраструктуры.

### auth.py

Задайте переменную окружения `WEBHOOK_SECRET` чтобы принимать запросы по адресу `/new-author` от (сервера авторизации)[https://dev.discours.io/devstack/authorizer]. Событие ожидается при создании нового пользователя. Для авторизованных запросов и мутаций фронтенд добавляет к запросу токен авторизации в заголовок `Authorization`.

### viewed.py

Задайте переменные окружения `GOOGLE_ANALYTICS_TOKEN` и `GOOGLE_GA_VIEW_ID` для сбора данных из Google Analytics.

### search.py

Результаты ElasticSearch с оценкой `score`, объединенные с запросами к базе данных, запрашиваем через GraphQL API `load_shouts_search`.

### notify.py

Отправка уведомлений по Redis PubSub каналам, согласно структуре данных, за которую отвечает [сервис уведомлений](https://dev.discours.io/discours.io/notifier)

###  unread.py

Счетчик непрочитанных сообщений получается через Redis-запрос к данным сервиса сообщений.

### following.py

Внутренний сервис, обеспечивающий асинхронный доступ к оперативному хранилищу подписчиков на комментарии, топики и авторы.
