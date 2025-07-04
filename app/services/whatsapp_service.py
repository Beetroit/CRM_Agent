import json
import logging
from datetime import datetime
from typing import Any, Dict
from urllib.parse import parse_qs, unquote

from app.models.whatsapp import WhatsAppQueueMessage
from app.services.rabbitmq_service import AsyncRabbitMQProducer

logger = logging.getLogger(__name__)


class WhatsAppService:
    def __init__(self, rabbitmq_producer: AsyncRabbitMQProducer):
        self.rabbitmq_producer = rabbitmq_producer

    @staticmethod
    def parse_webhook_data(raw_data: bytes) -> Dict[str, Any]:
        """
        Parse URL-encoded webhook data from Twilio WhatsApp webhook.

        Args:
            raw_data: Raw bytes from the webhook request body

        Returns:
            Dictionary containing parsed webhook data
        """
        try:
            # Decode the URL-encoded data
            decoded_data = raw_data.decode("utf-8")
            parsed_data = parse_qs(decoded_data, keep_blank_values=True)

            # Convert list values to single values (parse_qs returns lists)
            webhook_data = {
                key: value[0] if value else "" for key, value in parsed_data.items()
            }

            # Parse JSON fields if they exist
            if "ChannelMetadata" in webhook_data:
                try:
                    webhook_data["ChannelMetadata"] = json.loads(
                        unquote(webhook_data["ChannelMetadata"])
                    )
                except (json.JSONDecodeError, ValueError) as e:
                    logger.warning(f"Failed to parse ChannelMetadata JSON: {e}")

            # URL decode specific fields that might contain encoded characters
            fields_to_decode = ["ProfileName", "Body", "From", "To"]
            for field in fields_to_decode:
                if field in webhook_data:
                    webhook_data[field] = unquote(webhook_data[field])

            return webhook_data

        except Exception as e:
            logger.error(f"Failed to parse webhook data: {e}")
            raise ValueError(f"Invalid webhook data format: {e}")

    @staticmethod
    def determine_webhook_type(webhook_data: Dict[str, Any]) -> str:
        """
        Determine if this is a message webhook or status webhook.

        Args:
            webhook_data: Parsed webhook data dictionary

        Returns:
            "message" for incoming messages, "status" for delivery status updates
        """
        # Status webhooks have MessageStatus but no Body/WaId
        if "MessageStatus" in webhook_data and "Body" not in webhook_data:
            return "status"
        # Message webhooks have Body and WaId
        elif "Body" in webhook_data and "WaId" in webhook_data:
            return "message"
        else:
            return "unknown"

    @staticmethod
    def extract_message_info(webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant message information from parsed webhook data.

        Args:
            webhook_data: Parsed webhook data dictionary

        Returns:
            Dictionary containing extracted message information
        """
        webhook_type = WhatsAppService.determine_webhook_type(webhook_data)

        return {
            "message_sid": webhook_data.get("MessageSid"),
            "sms_message_sid": webhook_data.get("SmsMessageSid"),
            "account_sid": webhook_data.get("AccountSid"),
            "from_number": webhook_data.get("From"),
            "to_number": webhook_data.get("To"),
            "wa_id": webhook_data.get("WaId"),
            "profile_name": webhook_data.get("ProfileName"),
            "body": webhook_data.get("Body"),
            "message_type": webhook_data.get("MessageType"),
            "sms_status": webhook_data.get("SmsStatus"),
            "message_status": webhook_data.get("MessageStatus"),
            "num_media": int(webhook_data.get("NumMedia", 0))
            if webhook_data.get("NumMedia")
            else 0,
            "num_segments": int(webhook_data.get("NumSegments", 1))
            if webhook_data.get("NumSegments")
            else 1,
            "api_version": webhook_data.get("ApiVersion"),
            "channel_metadata": webhook_data.get("ChannelMetadata", {}),
            "channel_prefix": webhook_data.get("ChannelPrefix"),
            "channel_install_sid": webhook_data.get("ChannelInstallSid"),
            "channel_to_address": webhook_data.get("ChannelToAddress"),
            "structured_message": webhook_data.get("StructuredMessage"),
            "webhook_type": webhook_type,
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def process_incoming_message(self, webhook_data: Dict[str, Any]) -> None:
        """
        Process incoming WhatsApp message and queue it for further processing.

        Args:
            webhook_data: Parsed webhook data dictionary
        """
        try:
            message_info = self.extract_message_info(webhook_data)
            webhook_type = message_info["webhook_type"]

            logger.info(
                f"Processing WhatsApp {webhook_type} webhook - MessageSid: {message_info['message_sid']}"
            )

            # Only process actual messages, not status updates
            if webhook_type == "message":
                logger.info(
                    f"Processing message from {message_info['wa_id']}: {message_info['body']}"
                )

                # Create queue message
                queue_message = WhatsAppQueueMessage(
                    message_sid=message_info["message_sid"],
                    from_number=message_info["from_number"],
                    to_number=message_info["to_number"],
                    wa_id=message_info["wa_id"],
                    profile_name=message_info["profile_name"],
                    body=message_info["body"],
                    message_type=message_info["message_type"],
                    metadata=message_info,
                    webhook_type=webhook_type,
                )

                # Queue the message for processing
                await self.rabbitmq_producer.publish_message(
                    queue_name="whatsapp_messages", message=queue_message.model_dump()
                )

                logger.info(
                    f"Message queued successfully: {message_info['message_sid']}"
                )

            elif webhook_type == "status":
                logger.info(
                    f"Status update received: {message_info['message_status']} for MessageSid: {message_info['message_sid']}"
                )

                # Optionally queue status updates to a different queue
                # You might want to track delivery status in your system
                status_message = WhatsAppQueueMessage(
                    message_sid=message_info["message_sid"],
                    from_number=message_info["from_number"],
                    to_number=message_info["to_number"],
                    wa_id=None,  # Status updates don't have WaId
                    profile_name=None,
                    body=None,
                    message_type=message_info["message_status"],  # Use status as type
                    metadata=message_info,
                    webhook_type=webhook_type,
                )

                # Queue status updates to a separate queue
                await self.rabbitmq_producer.publish_message(
                    queue_name="whatsapp_status_updates",
                    message=status_message.model_dump(),
                )

                logger.info(f"Status update queued: {message_info['message_status']}")

            else:
                logger.warning(f"Unknown webhook type received: {webhook_type}")

        except Exception as e:
            logger.error(f"Failed to process incoming webhook: {e}")
            raise

    async def handle_webhook(self, raw_data: bytes) -> str:
        """
        Main webhook handler that orchestrates the entire process.

        Args:
            raw_data: Raw webhook data from Twilio

        Returns:
            TwiML response string
        """
        try:
            # Parse the webhook data
            webhook_data = self.parse_webhook_data(raw_data)

            # Log the parsed data for debugging (you might want to reduce this in production)
            logger.info(
                f"Parsed webhook data: {json.dumps(webhook_data, indent=2, default=str)}"
            )

            # Process the webhook
            await self.process_incoming_message(webhook_data)

            # Return TwiML response with a reply
            from twilio.twiml.messaging_response import MessagingResponse

            resp = MessagingResponse()
            # Add a simple reply message
            if webhook_data.get("Body"):
                resp.message(f"Message received: '{webhook_data['Body']}'")
            else:
                resp.message("Webhook received successfully!")

            return str(resp)

        except Exception as e:
            logger.error(f"Webhook processing failed: {e}")
            # Still return a valid TwiML response even if processing fails
            from twilio.twiml.messaging_response import MessagingResponse

            resp = MessagingResponse()
            resp.message("An error occurred while processing your message.")
            return str(resp)
