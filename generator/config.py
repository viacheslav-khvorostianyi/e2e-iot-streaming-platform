from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    bootstrap_servers: str = "broker:9092"
    topic: str = "household.power.readings"
    data_source: str = "/data/household/synthetic_data.csv"
    events_per_second: int = 100
    loop: bool = True

    class Config:
        env_file = ".env"


settings = Settings()
