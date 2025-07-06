# langgraph_app/workflows/whatsapp_workflow.py
import logging
from typing import List, Optional, TypedDict
from uuid import UUID

from langgraph.graph import END, StateGraph
from rich import print

from app.models.persistence import Message
from app.services.persistence_service import PersistenceService
from langgraph_app.services.llm_service import LLMService
from langgraph_app.services.vector_store_service import VectorStoreService
from langgraph_app.services.whatsapp_response_tool import WhatsAppResponseTool

logger = logging.getLogger(__name__)


class WorkflowState(TypedDict):
    message_id: str
    user_input: str
    from_number: str
    intent: str
    context: Optional[str]
    generated_response: str
    conversation_id: Optional[UUID]  # Add conversation_id to state


def create_whatsapp_workflow(
    llm_service: LLMService,
    vector_store_service: VectorStoreService,
    whatsapp_response_tool: WhatsAppResponseTool,
    persistence_service: PersistenceService,  # Add persistence_service
):
    async def start_node(state: WorkflowState) -> WorkflowState:
        logger.info("Starting workflow...")
        return state

    async def classify_intent_node(state: WorkflowState) -> WorkflowState:
        logger.info("Classifying intent...")
        intent = await llm_service.classify_intent(state["user_input"])
        state["intent"] = intent
        return state

    async def context_retrieval_node(state: WorkflowState) -> WorkflowState:
        logger.info("Retrieving context...")

        # The conversation_id is now passed directly into the state.
        # We can convert it to a UUID object for use.
        try:
            conversation_id = UUID(state["conversation_id"])
        except (ValueError, TypeError):
            logger.warning(
                f"Invalid or missing conversation_id in state: {state.get('conversation_id')}"
            )
            state["context"] = "No conversation history available."
            return state

        # Retrieve conversation history from PostgreSQL
        history: List[Message] = await persistence_service.get_conversation_history(
            conversation_id=conversation_id, limit=10
        )  # Limit to last 10 messages for context

        # Format history for LLM
        formatted_history = []
        for msg in history:
            sender = "User" if msg.sender_type == "user" else "Bot"
            formatted_history.append(f"{sender}: {msg.content}")

        # Combine with vector store context if still desired
        context_parts = []
        if formatted_history:
            context_parts.append(
                "Conversation History:\n" + "\n".join(formatted_history)
            )

        if state["intent"] == "query":
            # Still use vector store for semantic search if intent is query
            context_docs = vector_store_service.query_messages(state["user_input"])
            print(f"Retrieved vector store documents: {context_docs}")
            # Join the document content, ensuring robust checking for valid docs
            vector_context_parts = [
                doc["document"]
                for doc in context_docs
                if isinstance(doc, dict)
                and "document" in doc
                and isinstance(doc["document"], str)
            ]
            if vector_context_parts:
                context_parts.append(
                    "Relevant Documents:\n" + "\n".join(vector_context_parts)
                )

        if context_parts:
            state["context"] = "\n\n".join(context_parts)
        else:
            state["context"] = "No available context"

        return state

    async def generate_response_node(state: WorkflowState) -> WorkflowState:
        logger.info("Generating response...")
        # Pass the full context (history + semantic search) to the LLM
        response = await llm_service.generate_response(
            intent=state["intent"],
            text=state["user_input"],
            context=state["context"],  # This now includes conversation history
        )
        state["generated_response"] = response
        return state

    async def send_response_node(state: WorkflowState) -> WorkflowState:
        """
        Sends the generated response back to the user via the WhatsAppResponseTool
        and saves the bot's message to the persistence service.
        """
        logger.info("Sending response...")

        await whatsapp_response_tool.publish_message(
            to_number=state["from_number"],
            response_text=state["generated_response"],
            original_message_id=UUID(
                state["message_id"]
            ),  # Pass the user's message UUID
            conversation_id=state["conversation_id"],  # Pass the conversation UUID
        )
        logger.info(f"Published message to queue for {state['from_number']}")

        return state  # Return state to allow END to be reached

    workflow = StateGraph(WorkflowState)
    workflow.add_node("start", start_node)
    workflow.add_node("classify_intent", classify_intent_node)
    workflow.add_node("context_retrieval", context_retrieval_node)
    workflow.add_node("generate_response", generate_response_node)
    workflow.add_node("send_response", send_response_node)

    workflow.set_entry_point("start")
    workflow.add_edge("start", "classify_intent")
    workflow.add_edge("classify_intent", "context_retrieval")
    workflow.add_edge("context_retrieval", "generate_response")
    workflow.add_edge("generate_response", "send_response")
    workflow.add_edge("send_response", END)

    return workflow.compile()
