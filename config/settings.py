from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# Redis (shared registry between gateway and consumer VMs)
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD") or None

# Gateway identity — unique per gateway instance
SERVER_ID: str = os.getenv("SERVER_ID", "gateway-1")

# Gateway internal base URL — used by the consumer to push messages back
# e.g. "http://10.0.0.1:8000"  (private IP of the gateway VM)
GATEWAY_INTERNAL_URL: str = os.getenv("GATEWAY_INTERNAL_URL", "http://localhost:8000")

# Cassandra
CASSANDRA_HOSTS: list[str] = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_PORT: int = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE: str = os.getenv("CASSANDRA_KEYSPACE", "titanchat")
CASSANDRA_USERNAME: str = os.getenv("CASSANDRA_USERNAME", "")
CASSANDRA_PASSWORD: str = os.getenv("CASSANDRA_PASSWORD", "")

# JWT Auth
JWT_SECRET: str = os.getenv("JWT_SECRET", "changeme-in-production")
JWT_ALGORITHM: str = os.getenv("JWT_ALGORITHM", "HS256")

# Rate limiting (messages per minute per user)
RATE_LIMIT_PER_MINUTE: int = int(os.getenv("RATE_LIMIT_PER_MINUTE", "60"))

# PostgreSQL (accounts: admins, agents, bots)
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "titanchat_accounts")
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "titanchat")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")

DATABASE_URL: str = os.getenv(
    "DATABASE_URL",
    f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
)
