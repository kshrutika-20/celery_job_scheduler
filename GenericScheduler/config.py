import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """
    Manages all application settings. Loads from environment variables and .env file.
    """
    # MongoDB connection settings. URI is preferred.
    MONGO_URI: str | None = None
    MONGO_HOST: str = "localhost"
    MONGO_PORT: int = 27017
    MONGO_USER: str | None = None
    MONGO_PASS: str | None = None

    # Your specific database and collection names
    MONGO_DB: str = "medallion"
    MONGO_SCHEDULER_COLLECTION: str = "apscheduler"
    MONGO_STATUS_COLLECTION: str = "job_statuses"

    # URL for the internal control API running on the scheduler pod
    SCHEDULER_CONTROL_API_URL: str = "http://localhost:9001"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create a single, importable settings instance
settings = Settings()