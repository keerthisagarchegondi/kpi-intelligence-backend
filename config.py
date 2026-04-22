import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """
    Configuration class for loading environment variables
    """
    # Database Configuration
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/kpi_intelligence"
    )
    
    # Application Settings
    APP_NAME = os.getenv("APP_NAME", "KPI Intelligence Backend")
    APP_VERSION = os.getenv("APP_VERSION", "1.0.0")
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    
    # API Settings
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    
    # CORS Settings
    ALLOWED_ORIGINS = os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost:3000,http://localhost:8000"
    ).split(",")
    
    # Environment
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

config = Config()
