import json
import logging
from typing import Any, Dict

import aio_pika
from aio_pika import DeliveryMode, Message
from aiormq.exceptions import ChannelInvalidStateError, ChannelPreconditionFailed

from app.config import settings

logger = logging.getLogger(__name__)


class AsyncRabbitMQProducer:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def _declare_queue_topology(self):
        """Declares all exchanges and queues for the producer."""
        #HACK: Unused vars are here to prevent precondition errors
        # Declare Dead Letter Exchange for requests
        requests_dlx = await self.channel.declare_exchange(
            settings.REQUESTS_DLX, durable=True
        )

        # Declare Parking Lot Queue for requests
        requests_parking_lot_queue = await self.channel.declare_queue(
            settings.REQUESTS_PARKING_LOT_QUEUE, durable=True
        )
        await requests_parking_lot_queue.bind(requests_dlx, routing_key="#")

        # Declare Retry Queue for requests
        requests_retry_queue = await self.channel.declare_queue(
            settings.REQUESTS_RETRY_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",  # Route back to default exchange
                "x-dead-letter-routing-key": settings.WHATSAPP_MESSAGES_QUEUE,
                "x-message-ttl": settings.REQUESTS_RETRY_DELAY_MS,
            },
        )

        # Declare main whatsapp_messages queue with DLX
        whatsapp_messages_queue = await self.channel.declare_queue(
            settings.WHATSAPP_MESSAGES_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": settings.REQUESTS_DLX,
                "x-dead-letter-routing-key": settings.REQUESTS_RETRY_QUEUE,
            },
        )

        # Declare Dead Letter Exchange for responses
        responses_dlx = await self.channel.declare_exchange(
            settings.RESPONSES_DLX_NAME, durable=True
        )

        # Declare Parking Lot Queue for responses
        responses_parking_lot_queue = await self.channel.declare_queue(
            settings.RESPONSES_PARKING_LOT_QUEUE_NAME, durable=True
        )
        await responses_parking_lot_queue.bind(responses_dlx, routing_key="#")

        # Declare Retry Queue for responses
        responses_retry_queue = await self.channel.declare_queue(
            settings.RESPONSES_RETRY_QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",  # Route back to default exchange
                "x-dead-letter-routing-key": settings.WHATSAPP_RESPONSES_QUEUE,
                "x-message-ttl": settings.RABBITMQ_RETRY_DELAY_MS,
            },
        )

        # Declare main whatsapp_responses queue with DLX
        whatsapp_responses_queue = await self.channel.declare_queue(
            settings.WHATSAPP_RESPONSES_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": settings.RESPONSES_DLX_NAME,
                "x-dead-letter-routing-key": settings.RESPONSES_RETRY_QUEUE_NAME,
            },
        )

        # Declare whatsapp_status_updates queue
        whatsapp_status_updates_queue = await self.channel.declare_queue(
            settings.WHATSAPP_STATUS_UPDATES_QUEUE, durable=True
        )

        logger.info(
            "Successfully declared RabbitMQ topology: "
            + f"Main queues: {settings.WHATSAPP_MESSAGES_QUEUE}, {settings.WHATSAPP_RESPONSES_QUEUE}, {settings.WHATSAPP_STATUS_UPDATES_QUEUE}. \n"
            + f"Requests DLX: {settings.REQUESTS_DLX}, Retry Queue: {settings.REQUESTS_RETRY_QUEUE}, Parking Lot: {settings.REQUESTS_PARKING_LOT_QUEUE}. \n"
            + f"Responses DLX: {settings.RESPONSES_DLX_NAME}, Retry Queue: {settings.RESPONSES_RETRY_QUEUE_NAME}, Parking Lot: {settings.RESPONSES_PARKING_LOT_QUEUE_NAME}."
        )

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
            await self.channel.set_qos(prefetch_count=1)

            try:
                await self._declare_queue_topology()
            except ChannelPreconditionFailed as e:
                logger.warning(
                    f"Queue configuration mismatch detected: {e}. Attempting to recreate queue topology."
                )
                # The channel is likely in an invalid state, create a new one
                if self.channel and not self.channel.is_closed:
                    await self.channel.close()
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)

                # Delete problematic queues and exchanges, then re-declare the entire topology
                queues_to_delete = [
                    settings.WHATSAPP_MESSAGES_QUEUE,
                    settings.WHATSAPP_RESPONSES_QUEUE,
                    settings.WHATSAPP_STATUS_UPDATES_QUEUE,
                    settings.REQUESTS_RETRY_QUEUE,
                    settings.REQUESTS_PARKING_LOT_QUEUE,
                    settings.RESPONSES_RETRY_QUEUE_NAME,
                    settings.RESPONSES_PARKING_LOT_QUEUE_NAME,
                ]
                exchanges_to_delete = [
                    settings.REQUESTS_DLX,
                    settings.RESPONSES_DLX_NAME,
                ]

                for queue_name in queues_to_delete:
                    try:
                        await self.channel.queue_delete(queue_name)
                        logger.info(f"Deleted conflicting queue: {queue_name}")
                    except Exception as delete_e:
                        logger.warning(
                            f"Could not delete queue {queue_name}: {delete_e}"
                        )

                for exchange_name in exchanges_to_delete:
                    try:
                        await self.channel.exchange_delete(exchange_name)
                        logger.info(f"Deleted conflicting exchange: {exchange_name}")
                    except Exception as delete_e:
                        logger.warning(
                            f"Could not delete exchange {exchange_name}: {delete_e}"
                        )

                await self._declare_queue_topology()
                logger.info(
                    "Successfully re-declared RabbitMQ topology after precondition failure."
                )

            except ChannelInvalidStateError as e:
                logger.error(
                    f"Channel invalid state during RabbitMQ connection: {e}. Retrying connection."
                )
                # If the channel is in an invalid state, close it and try to reconnect
                if self.channel and not self.channel.is_closed:
                    await self.channel.close()
                await self.connect()  # Recursive call to retry connection

            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                raise
        finally:
            logger.info("Successfully connected to RabbitMQ.")

    async def publish_message(self, queue_name: str, message: Dict[str, Any]):
        """Publish a message to the specified queue."""
        try:
            if not self.channel or self.channel.is_closed:
                logger.warning("RabbitMQ channel not open. Attempting to reconnect.")
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
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("RabbitMQ connection closed.")
        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")
