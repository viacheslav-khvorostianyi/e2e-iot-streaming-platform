from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    bootstrap_servers: str = "broker:9092"
    input_topic: str = "household.power.readings"
    output_topic: str = "household.power.peaks"
    group_id: str = "peak-detector"
    window_size: int = 200
    min_window: int = 30
    sigma: float = 0.5          # IQR multiplier for the upper Tukey fence
    detection_feed: str = "grid_import"

    class Config:
        env_file = ".env"


settings = Settings()
