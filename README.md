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
poetry install
python3 server.py dev
```

# How to do an authorized request

Put the header 'Authorization' with token from signIn query or registerUser mutation.

# How to debug Ackee

Set ACKEE_TOKEN var

# test

