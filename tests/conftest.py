import asyncio
import os

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from main import app
from services.db import Base
from services.redis import redis
from settings import DB_URL

# Use SQLite for testing
TEST_DB_URL = "sqlite:///test.db"


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_engine():
    """Create a test database engine."""
    engine = create_engine(TEST_DB_URL)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    os.remove("test.db")


@pytest.fixture
def db_session(test_engine):
    """Create a new database session for a test."""
    connection = test_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
async def redis_client():
    """Create a test Redis client."""
    await redis.connect()
    yield redis
    await redis.disconnect()


@pytest.fixture
def test_client():
    """Create a TestClient instance."""
    return TestClient(app)
