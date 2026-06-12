"""
Password hashing (bcrypt) and JWT token creation/verification.
"""
import uuid
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

from config.settings import JWT_SECRET, JWT_ALGORITHM

# Token expiry
ACCESS_TOKEN_EXPIRE_HOURS = 24


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def create_access_token(
    user_id: str,
    role: str,
    admin_id: str | None = None,
    expires_delta: timedelta | None = None,
) -> str:
    """
    Create a JWT with user_id, role, and optionally admin_id.
    role: 'admin' | 'agent'
    admin_id is included for agents so we can scope queries.
    """
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    )
    payload = {
        "user_id": user_id,
        "role": role,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    if admin_id:
        payload["admin_id"] = admin_id
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    """
    Decode JWT. Raises jwt.ExpiredSignatureError or jwt.InvalidTokenError.
    """
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
