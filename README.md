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

Put the header 'Authorization' with token from signIn query or registerUser mutation. Setup `WEBHOOK_SECRET` env var

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
