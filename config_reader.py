from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr


class Settings(BaseSettings):
    bot_token: SecretStr
    mongo_url: SecretStr
    db_name: SecretStr
    collection_name: SecretStr
    
    model_config: SettingsConfigDict = SettingsConfigDict(
        env_file=".env",
    )


config = Settings()
