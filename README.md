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
```
redis-server
poetry env use 3.12
poetry install
poetry run python server.py dev
```
## Services

### Auth

Put the header 'Authorization' with token from signIn query or registerUser mutation.

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
