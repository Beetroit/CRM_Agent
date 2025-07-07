from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    WHATSAPP_MESSAGES_QUEUE: str = "whatsapp_messages"
    WHATSAPP_RESPONSES_QUEUE: str = "whatsapp_responses"
    WHATSAPP_STATUS_UPDATES_QUEUE: str = "whatsapp_status_updates"

    # DLQ and Retry settings for incoming requests
    REQUESTS_DLX: str = "requests_dlx"
    REQUESTS_RETRY_QUEUE: str = "requests_retry_queue"
    REQUESTS_PARKING_LOT_QUEUE: str = "requests_parking_lot"
    REQUESTS_RETRY_DELAY_MS: int = 5000  # 5 seconds

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
