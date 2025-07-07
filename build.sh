#!/bin/bash
set -e

# Set the image name and tag
IMAGE_NAME="revsprint-app"
TAG="latest"

# Build the Docker image
echo "Building Docker image: $IMAGE_NAME:$TAG"
docker build -t "$IMAGE_NAME:$TAG" .

echo "Build complete."
echo "To run the container, use the following command:"
echo "docker run -p 8000:8000 -p 5672:5672 -p 15672:15672 $IMAGE_NAME:$TAG"
