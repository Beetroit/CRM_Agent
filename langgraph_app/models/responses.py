# langgraph_app/models/responses.py
from pydantic import BaseModel


class WhatsAppResponseQueueMessage(BaseModel):
    to_number: str
    response_text: str
    original_message_sid: str
