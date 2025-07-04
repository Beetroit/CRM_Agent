import logging
import json
from typing import Dict, Any
import aio_pika
from aio_pika import Message, DeliveryMode

from app.config import settings

logger = logging.getLogger(__name__)


class AsyncRabbitMQProducer:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        """Establish connection to RabbitMQ and declare queues."""
        try:
            self.connection = await aio_pika.connect_robust(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                login=settings.RABBITMQ_USER,
                password=settings.RABBITMQ_PASSWORD,
            )
            self.channel = await self.connection.channel()

            # Declare queues
            await self.channel.declare_queue("whatsapp_messages", durable=True)
            await self.channel.declare_queue("whatsapp_status_updates", durable=True)
            logger.info(
                "Successfully connected to RabbitMQ and declared queues: whatsapp_messages, whatsapp_status_updates"
            )
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def publish_message(self, queue_name: str, message: Dict[str, Any]):
        """Publish a message to the specified queue."""
        try:
            if not self.channel:
                await self.connect()

            message_body = json.dumps(message, default=str)
            await self.channel.default_exchange.publish(
                Message(
                    message_body.encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key=queue_name,
            )
            logger.info(f"Message published to queue '{queue_name}': {message}")
        except Exception as e:
            logger.error(f"Failed to publish message to RabbitMQ: {e}")
            raise

    async def close(self):
        """Close the RabbitMQ connection."""
        try:
            if self.connection:
                await self.connection.close()
                logger.info("RabbitMQ connection closed.")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")
