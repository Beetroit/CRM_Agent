# langgraph_app/config.py
from pydantic_settings import BaseSettings


class LangGraphSettings(BaseSettings):
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    WHATSAPP_MESSAGES_QUEUE: str = "whatsapp_messages"
    WHATSAPP_RESPONSES_QUEUE: str = "whatsapp_responses"
    GOOGLE_API_KEY: str

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = LangGraphSettings()
