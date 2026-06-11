"""Unit tests for shared.auth.jwt — JWT authentication."""
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

import jwt as pyjwt
from fastapi import HTTPException

from shared.auth.jwt import decode_token, authenticate_ws, require_auth


TEST_SECRET = "test-secret"
TEST_ALGORITHM = "HS256"


def _make_token(payload: dict, secret: str = TEST_SECRET) -> str:
    return pyjwt.encode(payload, secret, algorithm=TEST_ALGORITHM)


@pytest.fixture(autouse=True)
def _patch_settings():
    with (
        patch("shared.auth.jwt.JWT_SECRET", TEST_SECRET),
        patch("shared.auth.jwt.JWT_ALGORITHM", TEST_ALGORITHM),
    ):
        yield


class TestDecodeToken:
    def test_valid_token(self):
        token = _make_token({"user_id": "user-1", "role": "customer"})
        payload = decode_token(token)
        assert payload["user_id"] == "user-1"
        assert payload["role"] == "customer"

    def test_expired_token(self):
        token = _make_token({"user_id": "user-1", "exp": int(time.time()) - 100})
        with pytest.raises(HTTPException) as exc_info:
            decode_token(token)
        assert exc_info.value.status_code == 401
        assert "expired" in exc_info.value.detail.lower()

    def test_invalid_token(self):
        with pytest.raises(HTTPException) as exc_info:
            decode_token("not.a.valid.token")
        assert exc_info.value.status_code == 401

    def test_missing_user_id(self):
        token = _make_token({"role": "customer"})
        with pytest.raises(HTTPException) as exc_info:
            decode_token(token)
        assert "user_id" in exc_info.value.detail

    def test_wrong_secret(self):
        token = _make_token({"user_id": "user-1"}, secret="wrong-secret")
        with pytest.raises(HTTPException):
            decode_token(token)


class TestAuthenticateWs:
    @pytest.mark.asyncio
    async def test_valid_token_in_query_params(self):
        token = _make_token({"user_id": "user-1", "role": "customer"})
        ws = AsyncMock()
        ws.query_params = {"token": token}

        result = await authenticate_ws(ws)

        assert result["user_id"] == "user-1"
        ws.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_token_closes_socket(self):
        ws = AsyncMock()
        ws.query_params = {}

        result = await authenticate_ws(ws)

        assert result == {}
        ws.close.assert_called_once_with(code=4001, reason="Missing token query param")

    @pytest.mark.asyncio
    async def test_invalid_token_closes_socket(self):
        ws = AsyncMock()
        ws.query_params = {"token": "invalid"}

        result = await authenticate_ws(ws)

        assert result == {}
        ws.close.assert_called_once()


class TestRequireAuth:
    @pytest.mark.asyncio
    async def test_valid_bearer_token(self):
        token = _make_token({"user_id": "user-1", "role": "admin"})
        request = MagicMock()
        request.headers = {"Authorization": f"Bearer {token}"}

        result = await require_auth(request)
        assert result["user_id"] == "user-1"

    @pytest.mark.asyncio
    async def test_missing_authorization_header(self):
        request = MagicMock()
        request.headers = {}

        with pytest.raises(HTTPException) as exc_info:
            await require_auth(request)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_non_bearer_token(self):
        request = MagicMock()
        request.headers = {"Authorization": "Basic abc123"}

        with pytest.raises(HTTPException):
            await require_auth(request)
