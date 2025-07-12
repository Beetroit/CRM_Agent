import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import parse_qs, unquote

from app.models.whatsapp import WhatsAppQueueMessage, WhatsAppWebhook
from app.services.persistence_service import PersistenceService
from app.services.rabbitmq_service import AsyncRabbitMQProducer
from app.services.vector_store_service import VectorStoreService

logger = logging.getLogger(__name__)


class WhatsAppService:
    def __init__(
        self,
        rabbitmq_producer: AsyncRabbitMQProducer,
        vector_store_service: VectorStoreService,
    ):
        self.rabbitmq_producer = rabbitmq_producer
        self.vector_store_service = vector_store_service

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
        Extract and clean relevant message information from parsed webhook data.
        Ensures all metadata values are compatible with ChromaDB.

        Args:
            webhook_data: Parsed webhook data dictionary

        Returns:
            Dictionary containing cleaned and extracted message information
        """
        webhook_type = WhatsAppService.determine_webhook_type(webhook_data)

        # Initial extraction
        message_info = {
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
            "num_media": webhook_data.get("NumMedia"),
            "num_segments": webhook_data.get("NumSegments"),
            "api_version": webhook_data.get("ApiVersion"),
            "channel_metadata": webhook_data.get("ChannelMetadata"),
            "channel_prefix": webhook_data.get("ChannelPrefix"),
            "channel_install_sid": webhook_data.get("ChannelInstallSid"),
            "channel_to_address": webhook_data.get("ChannelToAddress"),
            "structured_message": webhook_data.get("StructuredMessage"),
            "webhook_type": webhook_type,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }

        # Clean the dictionary to make it ChromaDB compatible
        cleaned_message_info = {}
        for key, value in message_info.items():
            if value is None:
                # Replace None with a sensible default
                if key in ["num_media", "num_segments"]:
                    cleaned_message_info[key] = 0
                else:
                    # Default to empty string for other None values
                    cleaned_message_info[key] = ""
            elif key == "channel_metadata" and isinstance(value, dict):
                # Serialize dicts to JSON strings
                cleaned_message_info[key] = json.dumps(value)
            elif key in ["num_media", "num_segments"]:
                # Ensure numeric fields are integers
                try:
                    cleaned_message_info[key] = int(value)
                except (ValueError, TypeError):
                    cleaned_message_info[key] = 0
            else:
                cleaned_message_info[key] = value

        return cleaned_message_info

    async def process_incoming_message(
        self, webhook_data: Dict[str, Any], persistence_service: PersistenceService
    ) -> None:
        """
        Process incoming WhatsApp message and queue it for further processing.

        Args:
            webhook_data: Parsed webhook data dictionary
            persistence_service: The service for database operations.
        """
        try:
            # Use the Pydantic model to parse and validate the webhook data
            # This will automatically handle the 'OriginalRepliedMessageSid' alias
            parsed_webhook = WhatsAppWebhook(**webhook_data)

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

                # Get or create conversation
                conversation = await persistence_service.get_or_create_conversation(
                    whatsapp_user_id=message_info["wa_id"]
                )
                # Eagerly load the conversation ID to prevent access after await
                conversation_id = conversation.id

                # Save message to PostgreSQL
                saved_message = await persistence_service.save_message(
                    message_sid=message_info["message_sid"],
                    conversation_id=conversation_id,
                    sender_id=message_info["wa_id"],
                    sender_type="user",
                    content=message_info["body"],
                    timestamp=datetime.fromisoformat(message_info["timestamp"]),
                    raw_metadata=webhook_data,  # Store the original raw webhook data
                    parent_message_sid=parsed_webhook.original_replied_message_sid,
                )
                # Eagerly load the message ID to prevent access after await
                saved_message_id = saved_message.id
                logger.info(
                    f"Message '{message_info['message_sid']}' saved to PostgreSQL with ID: {saved_message_id}"
                )

                # Create queue message, passing the database message ID
                queue_message = WhatsAppQueueMessage(
                    message_id=saved_message_id,  # Use the local variable
                    message_sid=message_info[
                        "message_sid"
                    ],  # Keep Twilio SID for reference
                    from_number=message_info["from_number"],
                    to_number=message_info["to_number"],
                    wa_id=message_info["wa_id"],
                    profile_name=message_info["profile_name"],
                    body=message_info["body"],
                    message_type=message_info["message_type"],
                    metadata=message_info,
                    webhook_type=webhook_type,
                    conversation_id=conversation_id,  # Use the local variable
                )

                # Queue the message for processing
                await self.rabbitmq_producer.publish_message(
                    queue_name="whatsapp_messages", message=queue_message.model_dump()
                )

                logger.info(
                    f"Message queued successfully: {message_info['message_sid']} (DB ID: {saved_message_id})"
                )

                # Add message to vector store
                await self.vector_store_service.add_message(
                    message_id=str(saved_message_id),  # Use local variable
                    content=message_info["body"],
                    metadata=message_info,
                )
                logger.info(
                    f"Message '{message_info['message_sid']}' added to vector store."
                )

            elif webhook_type == "status":
                logger.info(
                    f"Status update received: {message_info['message_status']} for MessageSid: {message_info['message_sid']}"
                )

                # Optionally queue status updates to a different queue
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

    async def handle_webhook(
        self, raw_data: bytes, persistence_service: PersistenceService
    ) -> str:
        """
        Main webhook handler that orchestrates the entire process.

        Args:
            raw_data: Raw webhook data from Twilio
            persistence_service: The service for database operations.

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
            await self.process_incoming_message(webhook_data, persistence_service)

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
