from datetime import datetime

import pytest

from orm.author import Author
from orm.shout import Shout


@pytest.fixture
def test_shout(db_session):
    """Create test shout with required fields."""
    now = int(datetime.now().timestamp())
    author = Author(name="Test Author", slug="test-author", user="test-user-id")
    db_session.add(author)
    db_session.flush()

    now = int(datetime.now().timestamp())

    shout = Shout(
        title="Test Shout",
        slug="test-shout",
        created_by=author.id,
        body="Test body",
        layout="article",
        lang="ru",
        community=1,
        created_at=now,
        updated_at=now,
    )
    db_session.add(shout)
    db_session.commit()
    return shout


@pytest.mark.asyncio
async def test_get_shout(test_client, db_session):
    """Test retrieving a shout."""
    # Создаем автора
    author = Author(name="Test Author", slug="test-author", user="test-user-id")
    db_session.add(author)
    db_session.flush()
    now = int(datetime.now().timestamp())

    # Создаем публикацию со всеми обязательными полями
    shout = Shout(
        title="Test Shout",
        body="This is a test shout",
        slug="test-shout",
        created_by=author.id,
        layout="article",
        lang="ru",
        community=1,
        created_at=now,
        updated_at=now,
    )
    db_session.add(shout)
    db_session.commit()

    response = test_client.post(
        "/",
        json={
            "query": """
            query GetShout($slug: String!) {
                get_shout(slug: $slug) {
                    id
                    title
                    body
                    created_at
                    updated_at
                    created_by {
                        id
                        name
                        slug
                    }
                }
            }
            """,
            "variables": {"slug": "test-shout"},
        },
    )

    data = response.json()
    assert response.status_code == 200
    assert "errors" not in data
    assert data["data"]["get_shout"]["title"] == "Test Shout"
