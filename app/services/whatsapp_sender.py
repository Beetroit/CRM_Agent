# app/services/whatsapp_sender.py
import asyncio
import json
import logging

import aio_pika
from aio_pika import IncomingMessage
from twilio.rest import Client

from app.config import settings
from app.models.whatsapp import WhatsAppResponseQueueMessage

logger = logging.getLogger(__name__)


class WhatsAppSenderService:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.twilio_client = Client(
            settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN
        )

    async def connect(self):
        try:
            self.connection = await aio_pika.connect_robust(
                host=settings.RABBITMQ_HOST,
                port=settings.RABBITMQ_PORT,
                login=settings.RABBITMQ_USER,
                password=settings.RABBITMQ_PASSWORD,
            )
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)
            logger.info("WhatsAppSenderService successfully connected to RabbitMQ.")
        except Exception as e:
            logger.error(f"WhatsAppSenderService failed to connect to RabbitMQ: {e}")
            raise

    async def on_message(self, message: IncomingMessage):
        async with message.process():
            try:
                data = json.loads(message.body.decode("utf-8"))
                response_message = WhatsAppResponseQueueMessage(**data)

                self.twilio_client.messages.create(
                    from_=f"whatsapp:{settings.TWILIO_PHONE_NUMBER}",
                    body=response_message.response_text,
                    to=response_message.to_number,
                )
                logger.info(f"Sent message to {response_message.to_number}")

            except json.JSONDecodeError:
                logger.error("Failed to decode message body as JSON.")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    async def start_consuming(self):
        if not self.connection or self.connection.is_closed:
            await self.connect()

        queue = await self.channel.declare_queue(
            settings.WHATSAPP_RESPONSES_QUEUE, durable=True
        )

        logger.info(
            f"Starting to consume from queue: {settings.WHATSAPP_RESPONSES_QUEUE}"
        )
        await queue.consume(self.on_message)

        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            logger.info("Consumer cancelled. Shutting down.")
        finally:
            await self.close()

    async def close(self):
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
