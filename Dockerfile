# Use an official Python runtime as a parent image
FROM python:slim

# Set the working directory in the container to /app
WORKDIR /app

# Add metadata to the image to describe that the container is listening on port 80
EXPOSE 80

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in pyproject.toml
RUN apt-get update && apt-get install -y gcc curl && \
    curl -sSL https://install.python-poetry.org | python - && \
    echo "export PATH=$PATH:/root/.local/bin" >> ~/.bashrc && \
    . ~/.bashrc && \
    poetry config virtualenvs.create false && \
    poetry install --no-dev

# Run server.py when the container launches
CMD ["python", "server.py"]