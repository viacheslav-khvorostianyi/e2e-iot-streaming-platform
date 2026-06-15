from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    bootstrap_servers: str = "broker:9092"
    input_topic: str = "household.power.peaks"
    group_id: str = "anomaly-reporter"
    from_beginning: bool = True

    max_events: int = 5000
    host: str = "0.0.0.0"
    port: int = 8050


settings = Settings()
