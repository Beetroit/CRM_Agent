# langgraph_app/services/rabbitmq_consumer_service.py
import asyncio
import json
import logging
from typing import Callable, Coroutine

import aio_pika
from aio_pika import IncomingMessage
from aiormq.exceptions import ChannelPreconditionFailed

from langgraph_app.config import LangGraphSettings

logger = logging.getLogger(__name__)


class AsyncRabbitMQConsumer:
    def __init__(self, settings: LangGraphSettings):
        self.settings = settings
        self.connection = None
        self.channel = None

    async def connect(self):
        try:
            self.connection = await aio_pika.connect_robust(
                host=self.settings.RABBITMQ_HOST,
                port=self.settings.RABBITMQ_PORT,
                login=self.settings.RABBITMQ_USER,
                password=self.settings.RABBITMQ_PASSWORD,
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)
            logger.info("Successfully connected to RabbitMQ.")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def on_message(
        self, message: IncomingMessage, workflow: Callable[..., Coroutine]
    ):
        try:
            message_data = json.loads(message.body.decode("utf-8"))
            logger.info(f"Received message: {message_data}")

            # Prepare the initial state for the LangGraph workflow
            initial_state = {
                "message_id": message_data.get("message_id"),
                "user_input": message_data.get("body"),
                "from_number": message_data.get("from_number"),
                "intent": "",
                "context": None,
                "generated_response": "",
                "conversation_id": message_data.get("conversation_id"),
            }

            await workflow.ainvoke(initial_state)
            await message.ack()
            logger.info(f"Successfully processed message {message.message_id}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

            # Check for retry count
            retries = 0
            if message.headers and "x-death" in message.headers:
                # x-death is a list of headers, one for each death event
                retries = len(message.headers["x-death"])

            if retries < self.settings.RABBITMQ_MAX_RETRIES:
                logger.warning(
                    f"Message failed. Retrying ({retries + 1}/{self.settings.RABBITMQ_MAX_RETRIES})..."
                )
                await message.nack(requeue=False)
            else:
                logger.error(
                    f"Message failed after {self.settings.RABBITMQ_MAX_RETRIES} retries. Moving to parking lot."
                )
                # Publish to parking lot
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message.body,
                        headers=message.headers,
                        delivery_mode=message.delivery_mode,
                    ),
                    routing_key=self.settings.REQUESTS_PARKING_LOT_QUEUE_NAME,
                )
                await message.ack()  # Acknowledge the original message to remove it

    async def start_consuming(self, workflow: Callable[..., Coroutine]):
        if not self.connection or self.connection.is_closed:
            await self.connect()

        # Declare the Dead Letter Exchange
        dlx = await self.channel.declare_exchange(
            self.settings.REQUESTS_DLX_NAME, aio_pika.ExchangeType.DIRECT
        )

        # Declare the Parking Lot Queue
        parking_lot_queue = await self.channel.declare_queue(
            self.settings.REQUESTS_PARKING_LOT_QUEUE_NAME, durable=True
        )

        # Declare the Retry Queue
        retry_queue = await self.channel.declare_queue(
            self.settings.REQUESTS_RETRY_QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",  # Default exchange
                "x-dead-letter-routing-key": self.settings.WHATSAPP_MESSAGES_QUEUE,
                "x-message-ttl": self.settings.RABBITMQ_RETRY_DELAY_MS,
            },
        )

        # Bind the retry queue to the DLX
        await retry_queue.bind(dlx, routing_key=self.settings.REQUESTS_RETRY_QUEUE_NAME)

        # Declare the main queue with DLX configured
        try:
            queue = await self.channel.declare_queue(
                self.settings.WHATSAPP_MESSAGES_QUEUE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.settings.REQUESTS_DLX_NAME,
                    "x-dead-letter-routing-key": self.settings.REQUESTS_RETRY_QUEUE_NAME,
                },
            )
        except ChannelPreconditionFailed:
            logger.warning(
                f"Queue {self.settings.WHATSAPP_MESSAGES_QUEUE} exists with different arguments. Recreating channel and queue."
            )
            # The channel is dead, create a new one
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            await self.channel.queue_delete(self.settings.WHATSAPP_MESSAGES_QUEUE)
            
            # Re-declare all exchanges and queues on the new channel
            dlx = await self.channel.declare_exchange(
                self.settings.REQUESTS_DLX_NAME, aio_pika.ExchangeType.DIRECT
            )
            parking_lot_queue = await self.channel.declare_queue(
                self.settings.REQUESTS_PARKING_LOT_QUEUE_NAME, durable=True
            )
            retry_queue = await self.channel.declare_queue(
                self.settings.REQUESTS_RETRY_QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": self.settings.WHATSAPP_MESSAGES_QUEUE,
                    "x-message-ttl": self.settings.RABBITMQ_RETRY_DELAY_MS,
                },
            )
            await retry_queue.bind(dlx, routing_key=self.settings.REQUESTS_RETRY_QUEUE_NAME)

            queue = await self.channel.declare_queue(
                self.settings.WHATSAPP_MESSAGES_QUEUE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": self.settings.REQUESTS_DLX_NAME,
                    "x-dead-letter-routing-key": self.settings.REQUESTS_RETRY_QUEUE_NAME,
                },
            )

        logger.info(
            f"Starting to consume from queue: {self.settings.WHATSAPP_MESSAGES_QUEUE}"
        )
        await queue.consume(lambda message: self.on_message(message, workflow))

        try:
            # Keep the consumer running indefinitely
            await asyncio.Future()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled. Shutting down.")
        finally:
            await self.close()

    async def close(self):
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
            logger.info("RabbitMQ channel closed.")
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")
