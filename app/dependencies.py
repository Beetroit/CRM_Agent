import logging

from fastapi import Request

from app.config import settings
from app.services.rabbitmq_service import AsyncRabbitMQProducer

logger = logging.getLogger(__name__)


# Dependency for RabbitMQ Producer
async def get_rabbitmq_producer() -> AsyncRabbitMQProducer:
    producer = AsyncRabbitMQProducer(
        hostname=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        username=settings.RABBITMQ_USER,
        password=settings.RABBITMQ_PASSWORD,
        queue_name=settings.RABBITMQ_QUEUE_NAME,
    )
    return producer


# To make the producer available to routes via dependency injection:
def get_producer_dependency(request: Request) -> AsyncRabbitMQProducer:
    return request.app.state.rabbitmq_producer
