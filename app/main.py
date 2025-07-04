import logging
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from scalar_fastapi import get_scalar_api_reference

from app.dependencies import get_rabbitmq_producer, get_vector_store_service
from app.routers import whatsapp_webhook
from app.services.vector_store_service import VectorStoreService
from app.services.whatsapp_sender import WhatsAppSenderService

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
        # If vector store fails, we might still want to proceed with RabbitMQ
        # depending on criticality. For now, we'll raise.
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
