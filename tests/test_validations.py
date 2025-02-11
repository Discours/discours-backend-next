from datetime import datetime, timedelta

import pytest
from pydantic import ValidationError

from auth.validations import (
    AuthInput,
    AuthResponse,
    TokenPayload,
    UserRegistrationInput,
)


class TestAuthValidations:
    def test_auth_input(self):
        """Test basic auth input validation"""
        # Valid case
        auth = AuthInput(user_id="123", username="testuser", token="1234567890abcdef1234567890abcdef")
        assert auth.user_id == "123"
        assert auth.username == "testuser"

        # Invalid cases
        with pytest.raises(ValidationError):
            AuthInput(user_id="", username="test", token="x" * 32)

        with pytest.raises(ValidationError):
            AuthInput(user_id="123", username="t", token="x" * 32)

    def test_user_registration(self):
        """Test user registration validation"""
        # Valid case
        user = UserRegistrationInput(email="test@example.com", password="SecurePass123!", name="Test User")
        assert user.email == "test@example.com"
        assert user.name == "Test User"

        # Test email validation
        with pytest.raises(ValidationError) as exc:
            UserRegistrationInput(email="invalid-email", password="SecurePass123!", name="Test")
        assert "Invalid email format" in str(exc.value)

        # Test password validation
        with pytest.raises(ValidationError) as exc:
            UserRegistrationInput(email="test@example.com", password="weak", name="Test")
        assert "String should have at least 8 characters" in str(exc.value)

    def test_token_payload(self):
        """Test token payload validation"""
        now = datetime.utcnow()
        exp = now + timedelta(hours=1)

        payload = TokenPayload(user_id="123", username="testuser", exp=exp, iat=now)
        assert payload.user_id == "123"
        assert payload.username == "testuser"
        assert payload.scopes == []  # Default empty list

    def test_auth_response(self):
        """Test auth response validation"""
        # Success case
        success_resp = AuthResponse(success=True, token="valid_token", user={"id": "123", "name": "Test"})
        assert success_resp.success is True
        assert success_resp.token == "valid_token"

        # Error case
        error_resp = AuthResponse(success=False, error="Invalid credentials")
        assert error_resp.success is False
        assert error_resp.error == "Invalid credentials"

        # Invalid case - отсутствует обязательное поле token при success=True
        with pytest.raises(ValidationError):
            AuthResponse(success=True, user={"id": "123", "name": "Test"})
