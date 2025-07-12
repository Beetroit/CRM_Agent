import logging
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlalchemy import asc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.persistence import Conversation, Message

logger = logging.getLogger(__name__)


class PersistenceService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_or_create_conversation(self, whatsapp_user_id: str) -> Conversation:
        """
        Retrieves an existing conversation or creates a new one for a given WhatsApp user ID.
        """
        result = await self.session.execute(
            select(Conversation).filter_by(whatsapp_user_id=whatsapp_user_id)
        )
        conversation = result.scalar_one_or_none()

        if not conversation:
            logger.info(f"Creating new conversation for user: {whatsapp_user_id}")
            conversation = Conversation(whatsapp_user_id=whatsapp_user_id)
            self.session.add(conversation)
            await self.session.commit()
            await self.session.refresh(conversation)
        else:
            logger.info(f"Found existing conversation for user: {whatsapp_user_id}")
        return conversation

    async def save_message(
        self,
        message_sid: str,
        conversation_id: UUID,
        sender_id: str,
        sender_type: str,
        content: Optional[str],
        timestamp: datetime,
        raw_metadata: dict,
        parent_message_sid: Optional[str] = None,
    ) -> Message:
        """
        Saves a new message to the database.
        """
        parent_message_id = None
        if parent_message_sid:
            parent_result = await self.session.execute(
                select(Message.id).filter_by(message_sid=parent_message_sid)
            )
            parent_message_id = parent_result.scalar_one_or_none()
            if not parent_message_id:
                logger.warning(
                    f"Parent message SID {parent_message_sid} not found for message {message_sid}. Storing without parent link."
                )

        message = Message(
            message_sid=message_sid,
            conversation_id=conversation_id,
            parent_message_id=parent_message_id,
            sender_id=sender_id,
            sender_type=sender_type,
            content=content,
            timestamp=timestamp,
            raw_metadata=raw_metadata,
        )
        self.session.add(message)
        await self.session.commit()
        await self.session.refresh(message)
        logger.info(f"Message {message_sid} saved to conversation {conversation_id}.")
        return message

    async def get_conversation_history(
        self, conversation_id: UUID, limit: int = 20
    ) -> List[Message]:
        """
        Retrieves the most recent messages for a given conversation, ordered chronologically.
        """
        result = await self.session.execute(
            select(Message)
            .filter_by(conversation_id=conversation_id)
            .order_by(asc(Message.timestamp))
            .limit(limit)
        )
        return list(result.scalars().all())

    async def save_bot_message(
        self,
        conversation_id: UUID,
        content: str,
        original_user_message_id: UUID,
        bot_message_sid: str,
    ) -> Message:
        """
        Saves a bot's response message, linking it to the original user message.
        """
        # The original_user_message_id is the UUID of the parent message.
        # We can use it directly as the parent_message_id.
        parent_message_id = original_user_message_id

        # Optional: You might want to log if the parent message ID is not found,
        # but for now, we'll trust it's passed correctly.
        # If you needed to verify:
        # parent_message = await self.session.get(Message, parent_message_id)
        # if not parent_message:
        #     logger.warning(f"Parent message with ID {parent_message_id} not found.")

        message = Message(
            message_sid=bot_message_sid,
            conversation_id=conversation_id,
            parent_message_id=parent_message_id,
            sender_id="bot",
            sender_type="bot",
            content=content,
            timestamp=datetime.now(),
            raw_metadata={},  # Bot messages might not have raw webhook metadata
        )
        self.session.add(message)
        await self.session.commit()
        await self.session.refresh(message)
        logger.info(
            f"Bot message {bot_message_sid} saved to conversation {conversation_id}."
        )
        return message
