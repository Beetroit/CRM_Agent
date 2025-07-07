# Use an official Python runtime as a parent image
FROM python:3.12-slim AS builder

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_VERSION=1.8.2

# Install poetry
RUN pip install "poetry==$POETRY_VERSION"

# Set the working directory in the container
WORKDIR /app

# Configure poetry to create the virtual environment in the project's root
RUN poetry config virtualenvs.in-project true

# Copy the poetry files into the container
COPY pyproject.toml ./

# Install dependencies
RUN poetry install --no-root
RUN poetry add protobuf==3.20.3

# Create a new stage for the final image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    rabbitmq-server \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv .venv

# Activate the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Copy the rest of the application's code into the container
COPY . .

# Copy the supervisord configuration file
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose the port that the FastAPI application will run on
EXPOSE 8000
EXPOSE 5672
EXPOSE 15672

# Start supervisord
CMD ["/usr/bin/supervisord"]