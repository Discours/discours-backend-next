FROM python:alpine

# Update package lists and install necessary dependencies
RUN apk update && \
    apk add --no-cache build-base icu-data-full curl python3-dev musl-dev postgresql-dev postgresql-client && \
    curl -sSL https://install.python-poetry.org | python

# Set working directory
WORKDIR /app

# Copy just the dependency manifests first
COPY poetry.lock pyproject.toml /app/

# Install dependencies
RUN poetry config virtualenvs.create false && \
    poetry install --no-dev

# Copy the rest of the application
COPY . /app

# Expose the port
EXPOSE 8000

# Command to run the application
CMD ["python", "server.py"]
