# langgraph_app/services/rabbitmq_consumer_service.py
import asyncio
import json
import logging
from typing import Callable, Coroutine

import aio_pika
from aio_pika import IncomingMessage

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
        async with message.process():
            try:
                message_data = json.loads(message.body.decode("utf-8"))
                logger.info(f"Received message: {message_data}")

                # Prepare the initial state for the LangGraph workflow
                initial_state = {
                    "message_id": message_data.get("message_sid"),
                    "user_input": message_data.get("body"),
                    "from_number": message_data.get("from_number"),
                    "intent": "",  # Will be classified by the workflow
                    "context": None,
                    "generated_response": "",
                }

                # Invoke the LangGraph workflow with the prepared state
                await workflow.ainvoke(initial_state)
            except json.JSONDecodeError:
                logger.error("Failed to decode message body as JSON.")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    async def start_consuming(self, workflow: Callable[..., Coroutine]):
        if not self.connection or self.connection.is_closed:
            await self.connect()

        queue = await self.channel.declare_queue(
            self.settings.WHATSAPP_MESSAGES_QUEUE, durable=True
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
