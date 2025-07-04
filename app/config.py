from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASSWORD: str = "guest"
    RABBITMQ_QUEUE_NAME: str = "whatsapp_messages"

    WHATSAPP_VERIFY_TOKEN: str

settings = Settings()