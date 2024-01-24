FROM python:slim
WORKDIR /app

EXPOSE 8080
ADD nginx.conf.sigil ./
COPY requirements.txt .
RUN apt update && apt install -y git gcc curl
RUN pip install -r requirements.txt
COPY . .
