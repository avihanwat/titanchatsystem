"""
TitanChat API Server — Admin, Agent, Bot management + Chat History.

Run:
    uvicorn api.main:app --host 0.0.0.0 --port 8001
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.database import engine
from api.models import Base
from api.routers import auth, bots, agents, conversations, dashboard, ws_dashboard
from shared.observability.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup (dev convenience — use alembic in production)
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("API server started — PostgreSQL tables ensured")
    except Exception as e:
        logger.warning("Could not connect to PostgreSQL on startup: %s (API will start without DB)", e)
    yield
    try:
        await engine.dispose()
    except Exception:
        pass
    logger.info("API server shut down")


app = FastAPI(
    title="TitanChat API",
    description="Admin, Agent, Bot management and Chat History API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(auth.router)
app.include_router(bots.router)
app.include_router(agents.router)
app.include_router(conversations.router)
app.include_router(dashboard.router)
app.include_router(ws_dashboard.router)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "titanchat-api"}
