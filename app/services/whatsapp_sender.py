# app/services/whatsapp_sender.py
import asyncio
import json
import logging

import aio_pika
from aio_pika import IncomingMessage
from aiormq.exceptions import ChannelPreconditionFailed
from twilio.rest import Client

from app.config import settings
from app.models.whatsapp import WhatsAppResponseQueueMessage
from app.services.database import get_session
from app.services.persistence_service import PersistenceService

logger = logging.getLogger(__name__)


class WhatsAppSenderService:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.twilio_client = Client(
            settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN
        )

    async def connect(self):
        logger.info("WhatsAppSenderService: Attempting to connect to RabbitMQ...")
        try:
            self.connection = await aio_pika.connect_robust(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                login=settings.RABBITMQ_USER,
                password=settings.RABBITMQ_PASSWORD,
            )
            logger.info("WhatsAppSenderService: RabbitMQ connection established.")
            self.channel = await self.connection.channel()
            logger.info("WhatsAppSenderService: RabbitMQ channel opened.")
            await self.channel.set_qos(prefetch_count=1)
            logger.info("WhatsAppSenderService: RabbitMQ QoS set. Connection successful.")
        except Exception as e:
            logger.error(f"WhatsAppSenderService failed to connect to RabbitMQ: {e}", exc_info=True)
            raise

    async def on_message(self, message: IncomingMessage):
        logger.info(f"Received message from queue: {message.message_id}")
        try:
            data = json.loads(message.body.decode("utf-8"))
            logger.info(f"Decoded message body: {data}")
            response_message = WhatsAppResponseQueueMessage(**data)
            logger.info(
                f"Parsed response message: {response_message.model_dump_json()}"
            )

            # Send the message via Twilio (run in a separate thread to avoid blocking)
            twilio_message = await asyncio.to_thread(
                self.twilio_client.messages.create,
                from_=f"whatsapp:{settings.TWILIO_PHONE_NUMBER}",
                body=response_message.response_text,
                to=response_message.to_number,
            )
            logger.info(
                f"Sent message to {response_message.to_number} with SID: {twilio_message.sid}"
            )

            # Save the bot's message to the database
            async with get_session() as session:
                persistence_service = PersistenceService(session)
                await persistence_service.save_bot_message(
                    conversation_id=response_message.conversation_id,
                    content=twilio_message.body,
                    original_user_message_id=response_message.original_message_id,
                    bot_message_sid=twilio_message.sid,
                )
                logger.info(f"Bot message {twilio_message.sid} saved to persistence.")

            await message.ack()
            logger.info(f"Successfully processed response message {message.message_id}")

        except Exception as e:
            logger.error(f"Error processing response message: {e}", exc_info=True)

            retries = 0
            if message.headers and "x-death" in message.headers:
                retries = len(message.headers["x-death"])

            if retries < settings.RABBITMQ_MAX_RETRIES:
                logger.warning(
                    f"Response message failed. Retrying ({retries + 1}/{settings.RABBITMQ_MAX_RETRIES})..."
                )
                await message.nack(requeue=False)
            else:
                logger.error(
                    f"Response message failed after {settings.RABBITMQ_MAX_RETRIES} retries. Moving to parking lot."
                )
                await self.channel.default_exchange.publish(
                    aio_pika.Message(
                        body=message.body,
                        headers=message.headers,
                        delivery_mode=message.delivery_mode,
                    ),
                    routing_key=settings.RESPONSES_PARKING_LOT_QUEUE_NAME,
                )
                await message.ack()

    async def start_consuming(self):
        if not self.connection or self.connection.is_closed:
            await self.connect()

        logger.info("Declaring RabbitMQ topology for WhatsAppSenderService...")

        # Declare the Dead Letter Exchange for responses
        dlx = await self.channel.declare_exchange(
            settings.RESPONSES_DLX_NAME, aio_pika.ExchangeType.DIRECT, durable=True
        )
        logger.info(f"Declared DLX: {settings.RESPONSES_DLX_NAME}")

        # Declare the Parking Lot Queue for responses
        await self.channel.declare_queue(
            settings.RESPONSES_PARKING_LOT_QUEUE_NAME, durable=True
        )
        logger.info(
            f"Declared Parking Lot Queue: {settings.RESPONSES_PARKING_LOT_QUEUE_NAME}"
        )

        # Declare the Retry Queue for responses
        retry_queue = await self.channel.declare_queue(
            settings.RESPONSES_RETRY_QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",  # Default exchange
                "x-dead-letter-routing-key": settings.WHATSAPP_RESPONSES_QUEUE,
                "x-message-ttl": settings.RABBITMQ_RETRY_DELAY_MS,
            },
        )
        await retry_queue.bind(dlx, routing_key=settings.RESPONSES_RETRY_QUEUE_NAME)
        logger.info(
            f"Declared and bound Retry Queue: {settings.RESPONSES_RETRY_QUEUE_NAME}"
        )

        # Declare the main responses queue with DLX configured
        try:
            queue = await self.channel.declare_queue(
                settings.WHATSAPP_RESPONSES_QUEUE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": settings.RESPONSES_DLX_NAME,
                    "x-dead-letter-routing-key": settings.RESPONSES_RETRY_QUEUE_NAME,
                },
            )
            logger.info(
                f"Declared main responses queue: {settings.WHATSAPP_RESPONSES_QUEUE}"
            )
        except ChannelPreconditionFailed:
            logger.warning(
                f"Queue {settings.WHATSAPP_RESPONSES_QUEUE} exists with different arguments. Recreating channel and queue."
            )
            # The channel is dead, create a new one
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

            await self.channel.queue_delete(settings.WHATSAPP_RESPONSES_QUEUE)

            # Re-declare all exchanges and queues on the new channel
            dlx = await self.channel.declare_exchange(
                settings.RESPONSES_DLX_NAME, aio_pika.ExchangeType.DIRECT
            )
            await self.channel.declare_queue(
                settings.RESPONSES_PARKING_LOT_QUEUE_NAME, durable=True
            )
            retry_queue = await self.channel.declare_queue(
                settings.RESPONSES_RETRY_QUEUE_NAME,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "",
                    "x-dead-letter-routing-key": settings.WHATSAPP_RESPONSES_QUEUE,
                    "x-message-ttl": settings.RABBITMQ_RETRY_DELAY_MS,
                },
            )
            await retry_queue.bind(dlx, routing_key=settings.RESPONSES_RETRY_QUEUE_NAME)

            queue = await self.channel.declare_queue(
                settings.WHATSAPP_RESPONSES_QUEUE,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": settings.RESPONSES_DLX_NAME,
                    "x-dead-letter-routing-key": settings.RESPONSES_RETRY_QUEUE_NAME,
                },
            )

        logger.info(
            f"Declared main responses queue: {settings.WHATSAPP_RESPONSES_QUEUE}"
        )
        logger.info(
            f"Starting to consume from queue: {settings.WHATSAPP_RESPONSES_QUEUE}"
        )
        logger.info("Attempting to consume messages with on_message callback...")

        consumer_tag = await queue.consume(self.on_message)
        logger.info(f"Consumer started with tag: {consumer_tag}")

        try:
            # Keep the consumer running indefinitely
            await asyncio.Future()
        except asyncio.CancelledError:
            logger.info(f"Consumer {consumer_tag} cancelled. Shutting down.")
            await queue.cancel(consumer_tag)
        finally:
            logger.info("Closing connection...")
            await self.close()

    async def close(self):
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
