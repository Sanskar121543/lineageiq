from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # App
    environment: str = "development"
    log_level: str = "INFO"

    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "lineageiq_dev"
    neo4j_max_connection_pool_size: int = 50

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    blast_radius_cache_ttl: int = 900  # 15 minutes

    # Celery
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # Postgres
    postgres_dsn: str = "postgresql://lineageiq:lineageiq_dev@localhost:5432/lineageiq"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"

    # LLM
    openai_api_key: str = ""
    inference_model: str = "gpt-4o"
    text2cypher_model: str = "gpt-4o"
    llm_max_tokens: int = 2048
    llm_temperature: float = 0.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
