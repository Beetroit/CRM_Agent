from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, EmailStr, Field


# --- WhatsApp Message Types ---
class TextMessage(BaseModel):
    body: str


class ImageMessage(BaseModel):
    id: str
    mime_type: str
    sha256: str


class VideoMessage(BaseModel):
    id: str
    mime_type: str
    sha256: str


class AudioMessage(BaseModel):
    id: str
    mime_type: str
    sha256: str


class DocumentMessage(BaseModel):
    id: str
    mime_type: str
    filename: Optional[str]
    sha256: str


class StickerMessage(BaseModel):
    id: str
    mime_type: str
    sha256: str


class LocationMessage(BaseModel):
    latitude: float
    longitude: float
    name: Optional[str] = None
    address: Optional[str] = None


class ContactName(BaseModel):
    first_name: Optional[str] = None
    formatted_name: str
    last_name: Optional[str] = None
    middle_name: Optional[str] = None
    prefix: Optional[str] = None
    suffix: Optional[str] = None


class ContactPhone(BaseModel):
    phone: Optional[str] = None
    type: Optional[str] = None
    wa_id: Optional[str] = None


class ContactEmail(BaseModel):
    email: Optional[EmailStr] = None
    type: Optional[str] = None


class ContactAddress(BaseModel):
    city: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    state: Optional[str] = None
    street: Optional[str] = None
    type: Optional[str] = None
    zip: Optional[str] = None


class ContactOrg(BaseModel):
    company: Optional[str] = None
    department: Optional[str] = None
    title: Optional[str] = None


class ContactUrl(BaseModel):
    url: Optional[str] = None
    type: Optional[str] = None


class Contact(BaseModel):
    addresses: Optional[List[ContactAddress]] = None
    emails: Optional[List[ContactEmail]] = None
    name: ContactName
    org: Optional[ContactOrg] = None
    phones: Optional[List[ContactPhone]] = None
    urls: Optional[List[ContactUrl]] = None
    # Add other message types as needed (e.g., button, reaction, order, system, unsupported)


class MessageObject(BaseModel):
    from_: str = Field(..., alias="from")
    id: str
    timestamp: str
    type: str
    text: Optional[TextMessage] = None
    image: Optional[ImageMessage] = None
    video: Optional[VideoMessage] = None
    audio: Optional[AudioMessage] = None
    document: Optional[DocumentMessage] = None
    sticker: Optional[StickerMessage] = None
    location: Optional[LocationMessage] = None
    contacts: Optional[List[Contact]] = None
    # Add other message types as needed (e.g., button, reaction, order, system, unsupported)


class Metadata(BaseModel):
    display_phone_number: str
    phone_number_id: str


class Value(BaseModel):
    messaging_product: str
    metadata: Metadata
    messages: Optional[List[MessageObject]] = None
    contacts: Optional[List[Dict[str, Any]]] = None  # Contacts who sent the message
    statuses: Optional[List[Dict[str, Any]]] = None  # Message status updates


class Change(BaseModel):
    value: Value
    field: str


class Entry(BaseModel):
    id: str
    changes: List[Change]


class WhatsAppWebhook(BaseModel):
    """Model for incoming WhatsApp webhook data from Twilio"""

    message_sid: str
    sms_message_sid: Optional[str] = None
    account_sid: str
    from_number: str
    to_number: str
    wa_id: Optional[str] = None
    profile_name: Optional[str] = None
    body: Optional[str] = None
    message_type: Optional[str] = None
    sms_status: str
    num_media: int = 0
    num_segments: int = 1
    api_version: str
    channel_metadata: Optional[Dict[str, Any]] = None
    message_status: Optional[str] = None  # For status webhooks
    channel_prefix: Optional[str] = None
    channel_install_sid: Optional[str] = None
    channel_to_address: Optional[str] = None
    structured_message: Optional[str] = None


class WhatsAppQueueMessage(BaseModel):
    """Model for queuing WhatsApp messages for processing"""

    message_sid: str
    from_number: str
    to_number: str
    source: str = "whatsapp"
    wa_id: Optional[str] = None
    profile_name: Optional[str] = None
    body: Optional[str] = None
    message_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    webhook_type: str = "message"  # "message" or "status"


class WhatsAppResponseQueueMessage(BaseModel):
    """Model for queuing WhatsApp responses for sending"""

    to_number: str
    response_text: str
    original_message_sid: str
