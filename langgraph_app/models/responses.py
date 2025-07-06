# langgraph_app/models/responses.py
from uuid import UUID
from pydantic import BaseModel


class WhatsAppResponseQueueMessage(BaseModel):
    to_number: str
    response_text: str
    original_message_id: UUID  # Changed from original_message_sid
    conversation_id: UUID
