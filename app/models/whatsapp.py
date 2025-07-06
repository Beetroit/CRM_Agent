from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class WhatsAppWebhook(BaseModel):
    """Model for incoming WhatsApp webhook data from Twilio"""

    model_config = ConfigDict(populate_by_name=True)

    message_sid: str = Field(..., alias="MessageSid")
    sms_message_sid: Optional[str] = Field(None, alias="SmsMessageSid")
    account_sid: str = Field(..., alias="AccountSid")
    from_number: str = Field(..., alias="From")
    to_number: str = Field(..., alias="To")
    wa_id: Optional[str] = Field(None, alias="WaId")
    profile_name: Optional[str] = Field(None, alias="ProfileName")
    body: Optional[str] = Field(None, alias="Body")
    message_type: Optional[str] = Field(None, alias="MessageType")
    sms_status: str = Field(..., alias="SmsStatus")
    num_media: int = Field(0, alias="NumMedia")
    num_segments: int = Field(1, alias="NumSegments")
    api_version: str = Field(..., alias="ApiVersion")
    channel_metadata: Optional[Dict[str, Any]] = None
    message_status: Optional[str] = None  # For status webhooks
    channel_prefix: Optional[str] = None
    channel_install_sid: Optional[str] = None
    channel_to_address: Optional[str] = None
    structured_message: Optional[str] = None
    original_replied_message_sid: Optional[str] = Field(
        None, alias="OriginalRepliedMessageSid"
    )


class WhatsAppQueueMessage(BaseModel):
    """Model for queuing WhatsApp messages for processing"""

    message_id: Optional[UUID] = None  # Changed from message_sid: str, made optional
    message_sid: Optional[str] = None  # Keep Twilio SID for reference, made optional
    from_number: str
    to_number: str
    source: str = "whatsapp"
    wa_id: Optional[str] = None
    profile_name: Optional[str] = None
    body: Optional[str] = None
    message_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: str = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )
    webhook_type: str = "message"  # TODO: Make an enum
    conversation_id: Optional[UUID] = None  # Added conversation_id, made optional


class WhatsAppResponseQueueMessage(BaseModel):
    """Model for queuing WhatsApp responses for sending"""

    to_number: str
    response_text: str
    original_message_id: UUID
    conversation_id: UUID
