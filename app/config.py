from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mongo_url: str = "mongodb://localhost:27018"
    mongo_database: str = "ecommerce_insights"
    redis_url: str = "redis://localhost:6380/0"
    cache_ttl_seconds: int = Field(default=86400, ge=1)
    worker_concurrency: int = Field(default=2, ge=1, le=16)
    failure_probability: float = Field(default=0.1, ge=0, le=1)
    simulation_speed: float = Field(default=1.0, ge=0, le=1)
    max_stage_attempts: int = Field(default=3, ge=1, le=5)
