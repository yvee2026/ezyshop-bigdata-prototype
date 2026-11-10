"""Central configuration. Every setting comes from environment variables (see .env.example).

No credentials are hard-coded; defaults only cover non-secret tuning values so unit tests can
run without a .env file.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable {name} (see .env.example)")
    return value


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass(frozen=True)
class Config:
    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "kafka:9092"))
    kafka_partitions: int = field(default_factory=lambda: _int("KAFKA_PARTITIONS", 3))
    topic_clicks: str = field(default_factory=lambda: _env("TOPIC_CLICKS", "clicks"))
    topic_orders: str = field(default_factory=lambda: _env("TOPIC_ORDERS", "orders"))
    topic_reviews: str = field(default_factory=lambda: _env("TOPIC_REVIEWS", "reviews"))

    # Object storage / lake
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://seaweedfs:8333"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", ""))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", ""))
    lake_bucket: str = field(default_factory=lambda: _env("LAKE_BUCKET", "ezyshop-lake"))

    # Mongo
    mongo_user: str = field(default_factory=lambda: _env("MONGO_USER", ""))
    mongo_password: str = field(default_factory=lambda: _env("MONGO_PASSWORD", ""))
    mongo_host: str = field(default_factory=lambda: _env("MONGO_HOST", "mongo"))
    mongo_port: int = field(default_factory=lambda: _int("MONGO_PORT", 27017))
    mongo_db: str = field(default_factory=lambda: _env("MONGO_DB", "ezyshop"))

    # Postgres
    pg_user: str = field(default_factory=lambda: _env("POSTGRES_USER", ""))
    pg_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", ""))
    pg_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "postgres"))
    pg_port: int = field(default_factory=lambda: _int("POSTGRES_PORT", 5432))
    pg_db: str = field(default_factory=lambda: _env("POSTGRES_DB", "ezyshop"))

    # Privacy
    pseudo_salt: str = field(default_factory=lambda: _env("PSEUDO_SALT", ""))

    # Generator
    events_per_sec: int = field(default_factory=lambda: _int("EVENTS_PER_SEC", 200))
    burst_multiplier: int = field(default_factory=lambda: _int("BURST_MULTIPLIER", 10))
    num_customers: int = field(default_factory=lambda: _int("NUM_CUSTOMERS", 5000))
    random_seed: int = field(default_factory=lambda: _int("RANDOM_SEED", 42))

    # Spark
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[3]"))
    spark_driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "2g"))
    shuffle_partitions: int = field(default_factory=lambda: _int("SPARK_SHUFFLE_PARTITIONS", 6))
    window_minutes: int = field(default_factory=lambda: _int("WINDOW_MINUTES", 1))
    watermark_minutes: int = field(default_factory=lambda: _int("WATERMARK_MINUTES", 10))
    trigger_seconds: int = field(default_factory=lambda: _int("TRIGGER_SECONDS", 30))
    max_offsets_per_trigger: int = field(default_factory=lambda: _int("MAX_OFFSETS_PER_TRIGGER", 200000))

    # ---- derived helpers ----
    @property
    def lake(self) -> str:
        return f"s3a://{self.lake_bucket}"

    def path(self, *parts: str) -> str:
        """s3a path inside the lake, e.g. cfg.path('bronze', 'orders')."""
        return "/".join([self.lake, *parts])

    @property
    def mongo_uri(self) -> str:
        return (
            f"mongodb://{self.mongo_user}:{self.mongo_password}@{self.mongo_host}:{self.mongo_port}/"
            f"?authSource=admin"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def jdbc_props(self) -> dict[str, str]:
        return {"user": self.pg_user, "password": self.pg_password, "driver": "org.postgresql.Driver"}

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} dbname={self.pg_db} "
            f"user={self.pg_user} password={self.pg_password}"
        )


def get_config() -> Config:
    return Config()
