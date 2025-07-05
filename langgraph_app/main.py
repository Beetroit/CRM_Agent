# langgraph_app/main.py
import asyncio
import logging

from langgraph_app.config import LangGraphSettings
from langgraph_app.services.llm_service import LLMService
from langgraph_app.services.rabbitmq_consumer_service import AsyncRabbitMQConsumer
from langgraph_app.services.vector_store_service import VectorStoreService
from langgraph_app.services.whatsapp_response_tool import WhatsAppResponseTool
from langgraph_app.workflows.whatsapp_workflow import create_whatsapp_workflow

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    logger.info("Loading LangGraph configuration...")
    settings = LangGraphSettings()

    # Initialize services
    rabbitmq_consumer_service = AsyncRabbitMQConsumer(settings)
    llm_service = LLMService(settings)
    vector_store_service = VectorStoreService(settings)
    whatsapp_response_tool = WhatsAppResponseTool(settings)

    # Create the workflow
    whatsapp_workflow = create_whatsapp_workflow(
        llm_service=llm_service,
        vector_store_service=vector_store_service,
        whatsapp_response_tool=whatsapp_response_tool,
    )

    logger.info("Starting LangGraph RabbitMQ consumer...")
    await rabbitmq_consumer_service.start_consuming(whatsapp_workflow)


if __name__ == "__main__":
    asyncio.run(main())
