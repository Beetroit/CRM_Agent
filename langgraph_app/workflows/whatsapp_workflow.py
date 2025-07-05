# langgraph_app/workflows/whatsapp_workflow.py
import logging
from typing import Optional, TypedDict

from langgraph.graph import END, StateGraph
from rich import print

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


def create_whatsapp_workflow(
    llm_service: LLMService,
    vector_store_service: VectorStoreService,
    whatsapp_response_tool: WhatsAppResponseTool,
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
        if state["intent"] == "query":
            context_docs = vector_store_service.query_messages(state["user_input"])
            print(f"Retrieved context documents: {context_docs}")
            # Join the document content, ensuring robust checking for valid docs
            context_parts = [
                doc
                for doc in context_docs
                if isinstance(doc, str)
            ]
            if context_parts:
                state["context"] = "\n".join(context_parts)
            else:
                state["context"] = "No available context"
        else:
            state["context"] = None
        return state

    async def generate_response_node(state: WorkflowState) -> WorkflowState:
        logger.info("Generating response...")
        response = await llm_service.generate_response(
            intent=state["intent"],
            text=state["user_input"],
            context=state["context"],
        )
        state["generated_response"] = response
        return state

    async def send_response_node(state: WorkflowState) -> None:
        """
        Sends the generated response back to the user via the WhatsAppResponseTool.
        This node does not modify the state and returns None.
        """
        logger.info("Sending response...")
        await whatsapp_response_tool.publish_message(
            to_number=state["from_number"],
            response_text=state["generated_response"],
            original_message_sid=state["message_id"],
        )
        return state

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
