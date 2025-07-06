# langgraph_app/services/whatsapp_response_tool.py
import json
import logging
from uuid import UUID

import aio_pika
from langchain_core.tools import tool

from langgraph_app.config import LangGraphSettings

logger = logging.getLogger(__name__)


class WhatsAppResponseTool:
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
            logger.info("WhatsAppResponseTool successfully connected to RabbitMQ.")
        except Exception as e:
            logger.error(f"WhatsAppResponseTool failed to connect to RabbitMQ: {e}")
            raise

    async def publish_message(
        self,
        to_number: str,
        response_text: str,
        original_message_id: UUID,
        conversation_id: UUID,
    ):
        if not self.channel or self.channel.is_closed:
            await self.connect()

        # Prepare the message body using the new model structure
        message_body = {
            "to_number": to_number,
            "response_text": response_text,
            "original_message_id": str(original_message_id),
            "conversation_id": str(conversation_id),
        }

        try:
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message_body).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=self.settings.WHATSAPP_RESPONSES_QUEUE,
            )
            logger.info(
                f"Successfully published response to {self.settings.WHATSAPP_RESPONSES_QUEUE}"
            )
        except Exception as e:
            logger.error(f"Failed to publish message: {e}")

    async def close(self):
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()


@tool
async def send_whatsapp_response(
    to_number: str,
    response_text: str,
    original_message_id: UUID,
    conversation_id: UUID,
):
    """Sends a response back to the user via WhatsApp."""
    settings = LangGraphSettings()
    response_tool = WhatsAppResponseTool(settings)
    await response_tool.publish_message(
        to_number, response_text, original_message_id, conversation_id
    )
    await response_tool.close()
    return "Response sent to WhatsApp queue."
