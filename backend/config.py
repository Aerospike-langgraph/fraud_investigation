import os
from typing import Optional

class Settings:
    # Aerospike Graph Configuration
    AEROSPIKE_HOST: str = os.getenv("AEROSPIKE_HOST", "localhost")
    AEROSPIKE_PORT: int = int(os.getenv("AEROSPIKE_PORT", "8182"))
    
    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_TITLE: str = "Fraud Detection API"
    API_VERSION: str = "1.0.0"
    
    # CORS Configuration
    CORS_ORIGINS: list = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]
    
    # Security Configuration
    SECRET_KEY: str = os.getenv("SECRET_KEY", "your-secret-key-here")
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Database Configuration
    DATABASE_URL: Optional[str] = os.getenv("DATABASE_URL")
    
    # Fraud Detection Configuration
    FRAUD_THRESHOLD_HIGH: float = float(os.getenv("FRAUD_THRESHOLD_HIGH", "70.0"))
    FRAUD_THRESHOLD_MEDIUM: float = float(os.getenv("FRAUD_THRESHOLD_MEDIUM", "50.0"))
    FRAUD_THRESHOLD_LOW: float = float(os.getenv("FRAUD_THRESHOLD_LOW", "25.0"))
    
    # Sample Data Configuration
    DEFAULT_USERS: int = int(os.getenv("DEFAULT_USERS", "50"))
    DEFAULT_TRANSACTIONS: int = int(os.getenv("DEFAULT_TRANSACTIONS", "200"))

settings = Settings() 