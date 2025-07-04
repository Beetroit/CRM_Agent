import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from scalar_fastapi import get_scalar_api_reference

from app.dependencies import get_rabbitmq_producer
from app.routers import whatsapp_webhook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Context manager for application lifespan events.
    Initializes and closes RabbitMQ connection.
    """
    logger.info("Application startup: Initializing RabbitMQ producer...")
    producer = await get_rabbitmq_producer()
    try:
        await producer.connect()
        app.state.rabbitmq_producer = producer
        logger.info("RabbitMQ producer initialized successfully.")
        yield
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ producer: {e}")
        raise
    finally:
        logger.info("Application shutdown: Closing RabbitMQ connection...")
        if hasattr(app.state, "rabbitmq_producer") and app.state.rabbitmq_producer:
            await app.state.rabbitmq_producer.close()
            logger.info("RabbitMQ connection closed.")


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
