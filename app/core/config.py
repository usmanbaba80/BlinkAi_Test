from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database settings - PostgreSQL ONLY
    db_host: str
    db_port: int = 5432  # PostgreSQL default port
    db_user: str
    db_password: str
    db_name: str  # Changed from db_database to db_name for consistency
    db_pool_size: int = 32  # Increased from 32 for high concurrency
    db_ssl_ca: Optional[str] = None
    db_server_public_key: Optional[str] = None  # Path to MySQL server public key (PEM)
    
    # Database pool optimization settings
    db_connection_timeout: int = 30  # Increased from 10
    db_pool_reset_session: bool = True
    db_use_pure_python: bool = True
    db_pool_recycle: int = 3600  # Recycle connections every hour
    db_pool_pre_ping: bool = True  # Validate connections before use
    
    # AWS/S3 settings
    aws_access_key_id: str
    aws_secret_access_key: str
    s3_endpoint_url: str = "https://usc1.contabostorage.com"
    s3_bucket_name: str = "blinksearch-bucket"
    
    
    
    # Application settings
    base_path: str = "https://usc1.contabostorage.com/b5029b1fccae461a9111877e6bef9f81:blinksearch-bucket"
    log_level: str = "INFO"
    
    # Browser settings - CONSOLIDATED
    browser_pool_size: int = 5  # 5 concurrent browsers
    max_tabs_per_browser: int = 10  # 10 tabs per browser for higher capacity
    browser_headless: bool = True
    browser_launch_timeout: int = 15  # Reduced from 30 to speed up browser creation
    browser_launch_concurrent: int = 5  # Match browser pool size (5 browsers)
    browser_health_check_interval: int = 30  # Health check interval in seconds
    
    # Screenshot settings - CONSOLIDATED
    screenshot_timeout: int = 60  # Based on single request timing
    screenshot_max_concurrent: int = 50  # 50 concurrent screenshots (5 browsers × 10 tabs each)
    screenshot_enable_queuing: bool = True  # Enable intelligent queuing
    screenshot_queue_size: int = 100  # Reasonable queue for sustainable load
    screenshot_queue_timeout: int = 300  # Longer timeout for queued requests (5 minutes)
    screenshot_retry_attempts: int = 0  # SINGLE RETRY: Handle transient failures
    screenshot_enable_request_metrics: bool = True  # Enable request metrics tracking
    
    # HTTP Client Pool Configuration
    http_max_connections: int = 200  # Total HTTP connections in pool
    http_max_keepalive: int = 50  # Persistent connections to reuse
    http_keepalive_expiry: int = 30  # Connection lifetime (seconds)
    http_connect_timeout: int = 15  # Connection establishment timeout
    http_read_timeout: int = 60  # Response read timeout
    http_write_timeout: int = 10  # Request write timeout
    http_pool_timeout: int = 5  # Pool acquisition timeout

    # # API Security
    # api_auth_enabled: bool = False  # When True, require valid Bearer token on all endpoints
    # api_bearer_token: Optional[str] = None  # Set in .env: API_BEARER_TOKEN=your-secret-token

 
    class Config:
        env_file = ".env"
        case_sensitive = False

# Global settings instance
settings = Settings() 