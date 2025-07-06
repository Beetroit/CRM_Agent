from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_QUEUE_NAME: str = "whatsapp_messages"
    WHATSAPP_RESPONSES_QUEUE: str = "whatsapp_responses"

    # DLQ and Retry settings for outgoing responses
    RESPONSES_DLX_NAME: str = "responses_dlx"
    RESPONSES_RETRY_QUEUE_NAME: str = "responses_retry_queue"
    RESPONSES_PARKING_LOT_QUEUE_NAME: str = "responses_parking_lot"
    RABBITMQ_MAX_RETRIES: int = 3
    RABBITMQ_RETRY_DELAY_MS: int = 5000

    WHATSAPP_VERIFY_TOKEN: str

    TWILIO_ACCOUNT_SID: str
    TWILIO_AUTH_TOKEN: str
    TWILIO_PHONE_NUMBER: str

    DATABASE_URL: str = "sqlite+aiosqlite:///whatsapp_chat_db"

    # DATABASE_URL: str = (
    #     "postgresql+asyncpg://postgres:postgres@localhost:5432/whatsapp_chat_db"
    # )


settings = Settings()
