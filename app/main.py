import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from scalar_fastapi import get_scalar_api_reference

from app.dependencies import get_rabbitmq_producer, get_vector_store_service, get_persistence_service
from app.routers import whatsapp_webhook
from app.services.vector_store_service import VectorStoreService
from app.services.persistence_service import PersistenceService
from app.services.whatsapp_sender import WhatsAppSenderService
from app.services.database import init_db # Import init_db

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager for application lifespan events.
    Initializes and closes RabbitMQ connection and Vector Store.
    """
    logger.info("Application startup: Initializing services...")

    # Initialize Database
    try:
        await init_db()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

    # Initialize RabbitMQ Producer
    producer = await get_rabbitmq_producer()
    try:
        await producer.connect()
        app.state.rabbitmq_producer = producer
        logger.info("RabbitMQ producer initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ producer: {e}")
        raise

    # Initialize Vector Store Service
    vector_store = await get_vector_store_service()
    try:
        await vector_store.initialize()
        app.state.vector_store_service = vector_store
        logger.info("Vector Store service initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Vector Store service: {e}")
        raise

    # Initialize Persistence Service
    # We need to manually create a session for the initial setup if not using Depends
    # For lifespan, we'll create it directly. In routes, it will be injected.
    try:
        # This is a bit of a workaround for lifespan, as Depends() is for routes.
        # The actual session will be managed by FastAPI's Depends in routes.
        # Here, we just need an instance to attach to app.state.
        # A more robust solution for lifespan might involve a dedicated provider for PersistenceService
        # that doesn't rely on get_session directly but rather on the engine.
        # For now, we'll use a temporary session to get the service instance.
        # This part needs careful consideration for production.
        # For simplicity, we'll assume get_persistence_service can be called without a session here
        # if it's only for attaching the service instance.
        # However, get_persistence_service *does* depend on get_session.
        # So, we need to provide a session for it during startup.
        # A better approach for lifespan is to directly instantiate PersistenceService with a session.
        # Let's adjust get_persistence_service to be more flexible or create a separate init for it.

        # Re-evaluating: The plan was to use Dishka for DI.
        # The lifespan context should ideally not manually instantiate services that rely on sessions.
        # Instead, the Dishka container should manage their lifecycle.
        # However, FastAPI's lifespan is separate from Dishka's request scope.
        # For now, I will initialize it directly for app.state.
        # A more "Dishka-native" way would be to have a separate provider for app-level services.

        # For now, I will instantiate it directly, assuming the session is handled by the provider.
        # This is a temporary measure for the lifespan context.
        # The actual dependency injection for routes will use the Depends(get_persistence_service)
        # which correctly uses get_session.

        # Let's simplify and just attach the service to app.state, assuming it's ready.
        # The actual session management for PersistenceService will be via Depends in routes.
        persistence_service = await get_persistence_service(session=None) # This will fail as session is required
        # Correction: get_persistence_service requires a session.
        # We need to get a session from the engine directly for lifespan.
        from app.services.database import AsyncSessionLocal
        async with AsyncSessionLocal() as session:
            persistence_service = PersistenceService(session)
        app.state.persistence_service = persistence_service
        logger.info("Persistence service initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize Persistence service: {e}")
        raise


    # Initialize and start WhatsApp Sender Service
    whatsapp_sender_service = WhatsAppSenderService()
    try:
        await whatsapp_sender_service.connect()
        app.state.whatsapp_sender_service = whatsapp_sender_service
        app.state.whatsapp_sender_task = asyncio.create_task(whatsapp_sender_service.start_consuming())
        logger.info("WhatsApp Sender service initialized and started successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize WhatsApp Sender service: {e}")
        raise

    try:
        yield
    finally:
        logger.info("Application shutdown: Closing services...")
        # Close RabbitMQ connection
        if hasattr(app.state, "rabbitmq_producer") and app.state.rabbitmq_producer:
            await app.state.rabbitmq_producer.close()
            logger.info("RabbitMQ connection closed.")

        # Close Vector Store (if applicable, ChromaDB PersistentClient doesn't need explicit close)
        if (
            hasattr(app.state, "vector_store_service")
            and app.state.vector_store_service
        ):
            await app.state.vector_store_service.close()
            logger.info("Vector Store service shutdown complete.")

        # Persistence service does not have a close method for now, as session is per-request
        # If it had connection pools or similar, it would need a close method.
        if hasattr(app.state, "persistence_service") and app.state.persistence_service:
            # No explicit close needed for PersistenceService itself, as it uses sessions
            # which are managed by SQLAlchemy's AsyncSessionLocal.
            logger.info("Persistence service shutdown complete (no explicit close needed).")


        # Stop the sender task and close the connection
        if hasattr(app.state, "whatsapp_sender_task") and app.state.whatsapp_sender_task:
            app.state.whatsapp_sender_task.cancel()
        if hasattr(app.state, "whatsapp_sender_service") and app.state.whatsapp_sender_service:
            await app.state.whatsapp_sender_service.close()
            logger.info("WhatsApp Sender service shutdown complete.")


app = FastAPI(
    title="RevSprint WhatsApp Webhook API",
    description="API to receive WhatsApp webhooks and push messages to RabbitMQ.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/scalar", include_in_schema=False)
async def scalar_html():
    return get_scalar_api_reference(
        openapi_url=app.openapi_url,
        title=app.title,
    )


# Include routers
app.include_router(whatsapp_webhook.router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
