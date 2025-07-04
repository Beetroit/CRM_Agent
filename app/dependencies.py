import logging

from fastapi import Request

from app.config import settings
from app.services.rabbitmq_service import AsyncRabbitMQProducer
from app.services.vector_store_service import VectorStoreService

logger = logging.getLogger(__name__)


# Dependency for RabbitMQ Producer
async def get_rabbitmq_producer() -> AsyncRabbitMQProducer:
    producer = AsyncRabbitMQProducer()
    return producer

# Dependency for Vector Store Service
async def get_vector_store_service() -> VectorStoreService:
    vector_store = VectorStoreService()
    return vector_store


# To make the producer available to routes via dependency injection:
def get_producer_dependency(request: Request) -> AsyncRabbitMQProducer:
    return request.app.state.rabbitmq_producer

# To make the vector store service available to routes via dependency injection:
def get_vector_store_dependency(request: Request) -> VectorStoreService:
    return request.app.state.vector_store_service
