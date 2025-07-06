import logging

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.database import get_session  # Import the get_session dependency
from app.services.persistence_service import PersistenceService
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


# Dependency for Persistence Service
async def get_persistence_service(
    session: AsyncSession = Depends(get_session),
) -> PersistenceService:
    """
    Provides a PersistenceService instance with an injected database session.
    """
    return PersistenceService(session)


# To make the producer available to routes via dependency injection:
def get_producer_dependency(request: Request) -> AsyncRabbitMQProducer:
    return request.app.state.rabbitmq_producer


# To make the vector store service available to routes via dependency injection:
def get_vector_store_dependency(request: Request) -> VectorStoreService:
    return request.app.state.vector_store_service


# To make the persistence service available to routes via dependency injection:
def get_persistence_dependency(request: Request) -> PersistenceService:
    return request.app.state.persistence_service
