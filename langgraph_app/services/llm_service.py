# langgraph_app/services/llm_service.py
import logging

from langchain_google_genai import ChatGoogleGenerativeAI

from langgraph_app.config import LangGraphSettings

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self, settings: LangGraphSettings):
        self.settings = settings
        # General purpose LLM for generating responses
        self.llm = ChatGoogleGenerativeAI(
            model=self.settings.RESPONSE_MODEL_NAME,
            google_api_key=self.settings.GOOGLE_API_KEY,
        )
        # Lighter, faster model for classification
        self.intent_llm = ChatGoogleGenerativeAI(
            model=self.settings.INTENT_MODEL_NAME,
            google_api_key=self.settings.GOOGLE_API_KEY,
        )

    async def classify_intent(self, text: str) -> str:
        """Classifies the user's intent using a dedicated LLM."""
        logger.info(f"Classifying intent for: '{text}'")
        allowed_intents = ", ".join(self.settings.ALLOWED_INTENTS)
        prompt = (
            f"Based on the following user message, classify the intent into one of the following categories: "
            f"[{allowed_intents}].\n\n"
            f"User Message: '{text}'\n\n"
            f"Respond with only the intent category name and nothing else."
        )

        try:
            response = await self.intent_llm.ainvoke(prompt)
            # The 'content' can be a string or a list, handle both cases
            raw_content = response.content
            intent_text = ""
            if isinstance(raw_content, str):
                intent_text = raw_content
            elif isinstance(raw_content, list) and raw_content:
                # If it's a list, assume the first element is the text
                first_item = raw_content[0]
                if isinstance(first_item, str):
                    intent_text = first_item
                elif isinstance(first_item, dict) and "text" in first_item:
                    intent_text = first_item["text"]

            intent = intent_text.strip().lower()
            logger.info(f"LLM classified intent as: '{intent}'")

            # Validate if the returned intent is in the allowed list
            if intent in self.settings.ALLOWED_INTENTS:
                return intent
            else:
                logger.warning(
                    f"LLM returned an invalid intent: '{intent}'. Defaulting to 'statement'."
                )
                return "statement"  # Fallback to a default intent
        except Exception as e:
            logger.error(f"Error during intent classification: {e}")
            return "statement"  # Fallback on error

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
        logger.info(f"LLM generated response: {response.content}")
        return response.content
