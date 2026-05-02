"""
Application configuration using Pydantic Settings.
Loads configuration from environment variables and .env files.
"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import List
import os


class Settings(BaseSettings):
    """Central configuration for a distributed sync node."""

    # Node Identity
    node_id: str = Field(default="node1", description="Unique identifier for this node")
    node_host: str = Field(default="0.0.0.0", description="Host to bind the server to")
    node_port: int = Field(default=8001, description="Port to bind the server to")

    # Peer Nodes
    peer_nodes: str = Field(
        default="",
        description="Comma-separated list of peer node URLs"
    )

    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL"
    )

    # Security
    api_key: str = Field(default="dev-api-key-123", description="API key for client authentication")
    admin_key: str = Field(default="dev-admin-key-456", description="Admin API key")
    node_secret: str = Field(default="dev-node-secret-789", description="Shared secret for inter-node auth")

    # Raft Configuration
    election_timeout_min: int = Field(default=1500, description="Minimum election timeout in ms")
    election_timeout_max: int = Field(default=3000, description="Maximum election timeout in ms")
    heartbeat_interval: int = Field(default=500, description="Heartbeat interval in ms")

    # Cache Configuration
    cache_max_size: int = Field(default=1000, description="Maximum cache entries per node")
    cache_ttl: int = Field(default=300, description="Cache TTL in seconds")

    # Queue Configuration
    queue_ack_timeout: int = Field(default=30, description="Queue ack timeout in seconds")
    queue_virtual_nodes: int = Field(default=150, description="Virtual nodes per physical node in hash ring")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    def get_peer_list(self) -> List[str]:
        """Parse peer nodes string into a list of URLs."""
        if not self.peer_nodes:
            return []
        return [p.strip() for p in self.peer_nodes.split(",") if p.strip()]

    def get_node_url(self) -> str:
        """Get this node's URL."""
        return f"http://{self.node_host}:{self.node_port}"


# Singleton settings instance
_settings = None


def get_settings() -> Settings:
    """Get or create the settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
