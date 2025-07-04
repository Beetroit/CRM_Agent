import logging

from fastapi import Request

from app.config import settings
from app.services.rabbitmq_service import AsyncRabbitMQProducer

logger = logging.getLogger(__name__)


# Dependency for RabbitMQ Producer
async def get_rabbitmq_producer() -> AsyncRabbitMQProducer:
    producer = AsyncRabbitMQProducer()
    return producer


# To make the producer available to routes via dependency injection:
def get_producer_dependency(request: Request) -> AsyncRabbitMQProducer:
    return request.app.state.rabbitmq_producer
