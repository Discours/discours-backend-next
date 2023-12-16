# discoursio-core


- sqlalchemy
- redis
- ariadne
- starlette
- uvicorn

# Local development

Install deps first

on osx
```
brew install redis nginx postgres
brew services start redis
```

on debian/ubuntu
```
apt install redis nginx
```

Then run nginx, redis and API server

```shell
mkdir .venv
python3.12 -m venv .venv
poetry env use .venv/bin/python3.12
poetry update
poetry run python server.py
```
## Services

### Auth

Setup `WEBHOOK_SECRET` env var, webhook payload on `/new-author` is expected when User is created. In front-end put the header 'Authorization' with token from signIn query or registerUser mutation.

### Viewed

Set ACKEE_TOKEN var to collect stats

### Seacrh

ElasticSearch

### Notifications

Connected using Redis PubSub channels

### Inbox

To get unread counter raw redis query to Inbox's data is used


### Following Manager

Internal service with async access to storage
