"""
JWT authentication for WebSocket and HTTP endpoints.

Usage:
    # In WebSocket:
    user = await authenticate_ws(websocket)

    # In HTTP:
    @app.get("/api/...")
    async def endpoint(user: dict = Depends(require_auth)):
        ...
"""
import logging
from datetime import datetime, timezone

import jwt
from fastapi import HTTPException, Request, WebSocket, status

from config.settings import JWT_SECRET, JWT_ALGORITHM

logger = logging.getLogger(__name__)


def decode_token(token: str) -> dict:
    """
    Decode and validate JWT token.
    Returns the payload dict with at minimum: user_id, role.
    Raises HTTPException on invalid/expired tokens.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail=f"Invalid token: {exc}")

    if "user_id" not in payload:
        raise HTTPException(status_code=401, detail="Token missing user_id")

    return payload


async def authenticate_ws(websocket: WebSocket) -> dict:
    """
    Extract and validate JWT from WebSocket query params or first message.
    Expects: ws://host/ws/...?token=<JWT>
    Returns user payload dict.
    Closes WebSocket with 4001 if auth fails.
    """
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=4001, reason="Missing token query param")
        return {}

    try:
        payload = decode_token(token)
    except HTTPException as exc:
        await websocket.close(code=4001, reason=exc.detail)
        return {}

    return payload


async def require_auth(request: Request) -> dict:
    """
    FastAPI dependency for protected HTTP endpoints.
    Expects: Authorization: Bearer <token>
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth_header[7:]
    return decode_token(token)


async def require_role(request: Request, role: str) -> dict:
    """Verify user has the required role."""
    user = await require_auth(request)
    if user.get("role") != role:
        raise HTTPException(status_code=403, detail=f"Requires role: {role}")
    return user


def create_token(user_id: str, role: str = "customer", **extra) -> str:
    """
    Create a JWT token. Utility for testing and internal use.
    In production, tokens are issued by your auth service.
    """
    payload = {
        "user_id": user_id,
        "role": role,
        "iat": datetime.now(timezone.utc),
        **extra,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
