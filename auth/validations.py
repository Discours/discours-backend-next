import re
from datetime import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator

# RFC 5322 compliant email regex pattern
EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"


class AuthInput(BaseModel):
    """Base model for authentication input validation"""

    user_id: str = Field(description="Unique user identifier")
    username: str = Field(min_length=2, max_length=50)
    token: str = Field(min_length=32)

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("user_id cannot be empty")
        return v


class UserRegistrationInput(BaseModel):
    """Validation model for user registration"""

    email: str = Field(max_length=254)  # Max email length per RFC 5321
    password: str = Field(min_length=8, max_length=100)
    name: str = Field(min_length=2, max_length=50)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format"""
        if not re.match(EMAIL_PATTERN, v):
            raise ValueError("Invalid email format")
        return v.lower()

    @field_validator("password")
    @classmethod
    def validate_password_strength(cls, v: str) -> str:
        """Validate password meets security requirements"""
        if not any(c.isupper() for c in v):
            raise ValueError("Password must contain at least one uppercase letter")
        if not any(c.islower() for c in v):
            raise ValueError("Password must contain at least one lowercase letter")
        if not any(c.isdigit() for c in v):
            raise ValueError("Password must contain at least one number")
        if not any(c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in v):
            raise ValueError("Password must contain at least one special character")
        return v


class UserLoginInput(BaseModel):
    """Validation model for user login"""

    email: str = Field(max_length=254)
    password: str = Field(min_length=8, max_length=100)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        if not re.match(EMAIL_PATTERN, v):
            raise ValueError("Invalid email format")
        return v.lower()


class TokenPayload(BaseModel):
    """Validation model for JWT token payload"""

    user_id: str
    username: str
    exp: datetime
    iat: datetime
    scopes: Optional[List[str]] = []


class OAuthInput(BaseModel):
    """Validation model for OAuth input"""

    provider: str = Field(pattern="^(google|github|facebook)$")
    code: str
    redirect_uri: Optional[str] = None

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        valid_providers = ["google", "github", "facebook"]
        if v.lower() not in valid_providers:
            raise ValueError(f"Provider must be one of: {', '.join(valid_providers)}")
        return v.lower()


class AuthResponse(BaseModel):
    """Validation model for authentication responses"""

    success: bool
    token: Optional[str] = None
    error: Optional[str] = None
    user: Optional[Dict[str, Union[str, int, bool]]] = None

    @field_validator("error")
    @classmethod
    def validate_error_if_not_success(cls, v: Optional[str], info) -> Optional[str]:
        if not info.data.get("success") and not v:
            raise ValueError("Error message required when success is False")
        return v

    @field_validator("token")
    @classmethod
    def validate_token_if_success(cls, v: Optional[str], info) -> Optional[str]:
        if info.data.get("success") and not v:
            raise ValueError("Token required when success is True")
        return v
