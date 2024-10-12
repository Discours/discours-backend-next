FROM python:3.12-alpine

# Update package lists and install necessary dependencies
RUN apk update && \
    apk add --no-cache build-base icu-data-full curl python3-dev musl-dev && \
    curl -sSL https://install.python-poetry.org | python

# Set working directory
WORKDIR /app

# Copy only the pyproject.toml file initially
COPY pyproject.toml /app/

# Install poetry and dependencies
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --only main

# Copy the rest of the files
COPY . /app

# Expose the port
EXPOSE 8000

CMD ["python", "server.py"]
