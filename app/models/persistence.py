import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import JSON, UUID, Column, DateTime, ForeignKey, String, Text, func
from sqlalchemy.orm import relationship

from app.models.base import Base


class Conversation(Base):
    __tablename__ = "conversations"

    id: UUID = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    whatsapp_user_id: str = Column(String(255), unique=True, nullable=False)
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc)
    )
    updated_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=func.now()
    )

    messages = relationship("Message", back_populates="conversation")

    def __repr__(self):
        return (
            f"<Conversation(id={self.id}, whatsapp_user_id='{self.whatsapp_user_id}')>"
        )


class Message(Base):
    __tablename__ = "messages"

    id: UUID = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    message_sid: str = Column(String(255), unique=True, nullable=False)
    conversation_id: UUID = Column(
        UUID(as_uuid=True), ForeignKey("conversations.id"), nullable=False
    )
    parent_message_id: Optional[UUID] = Column(
        UUID(as_uuid=True), ForeignKey("messages.id"), nullable=True
    )
    sender_id: str = Column(String(255), nullable=False)
    sender_type: str = Column(String(50), nullable=False)  # 'user' or 'bot'
    content: Optional[str] = Column(Text, nullable=True)
    timestamp: datetime = Column(DateTime(timezone=True), nullable=False)
    raw_metadata: Optional[dict] = Column(JSON, nullable=True)
    created_at: datetime = Column(
        DateTime(timezone=True), default=datetime.now(timezone.utc)
    )

    conversation = relationship("Conversation", back_populates="messages")
    parent_message = relationship("Message", remote_side=[id], backref="replies")

    def __repr__(self):
        return (
            f"<Message(id={self.id}, message_sid='{self.message_sid}', "
            f"conversation_id={self.conversation_id}, sender_type='{self.sender_type}')>"
        )
