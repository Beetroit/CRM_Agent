# langgraph_app/services/llm_service.py
import logging

from langchain_google_genai import ChatGoogleGenerativeAI

from langgraph_app.config import LangGraphSettings

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self, settings: LangGraphSettings):
        self.settings = settings
        self.llm = ChatGoogleGenerativeAI(
            model="gemini/gemini-pro", google_api_key=self.settings.GOOGLE_API_KEY
        )

    async def classify_intent(self, text: str) -> str:
        # This is a placeholder for a more sophisticated intent classification
        logger.info(f"Classifying intent for: {text}")
        if "hello" in text.lower() or "hi" in text.lower():
            return "greeting"
        elif "?" in text:
            return "query"
        else:
            return "statement"

    async def generate_response(
        self, intent: str, text: str, context: str = None
    ) -> str:
        logger.info(f"Generating response for intent '{intent}' with text: {text}")

        prompt = f"Intent: {intent}\n"
        if context:
            prompt += f"Context: {context}\n"
        prompt += f"User message: {text}\n\n"
        prompt += "Generate a suitable response:"

        response = await self.llm.ainvoke(prompt)
        return response.content
