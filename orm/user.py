import time

from sqlalchemy import Boolean, Column, Integer, String

from services.db import Base


class User(Base):
    __tablename__ = "authorizer_users"

    id = Column(String, primary_key=True, unique=True, nullable=False, default=None)
    key = Column(String)
    email = Column(String, unique=True)
    email_verified_at = Column(Integer)
    family_name = Column(String)
    gender = Column(String)
    given_name = Column(String)
    is_multi_factor_auth_enabled = Column(Boolean)
    middle_name = Column(String)
    nickname = Column(String)
    password = Column(String)
    phone_number = Column(String, unique=True)
    phone_number_verified_at = Column(Integer)
    # preferred_username = Column(String, nullable=False)
    picture = Column(String)
    revoked_timestamp = Column(Integer)
    roles = Column(String, default="author,reader")
    signup_methods = Column(String, default="magic_link_login")
    created_at = Column(Integer, default=lambda: int(time.time()))
    updated_at = Column(Integer, default=lambda: int(time.time()))
