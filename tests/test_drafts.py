import pytest
from orm.shout import Shout
from orm.author import Author

@pytest.fixture
def test_author(db_session):
    """Create a test author."""
    author = Author(
        name="Test Author",
        slug="test-author",
        user="test-user-id"
    )
    db_session.add(author)
    db_session.commit()
    return author

@pytest.fixture
def test_shout(db_session):
    """Create test shout with required fields."""
    author = Author(name="Test Author", slug="test-author", user="test-user-id")
    db_session.add(author)
    db_session.flush()

    shout = Shout(
        title="Test Shout",
        slug="test-shout",
        created_by=author.id,  # Обязательное поле
        body="Test body",
        layout="article",
        lang="ru"
    )
    db_session.add(shout)
    db_session.commit()
    return shout

@pytest.mark.asyncio
async def test_create_shout(test_client, db_session, test_author):
    """Test creating a new shout."""
    response = test_client.post(
        "/",
        json={
            "query": """
            mutation CreateDraft($input: DraftInput!) {
                create_draft(input: $input) {
                    error
                    draft {
                        id
                        title
                        body
                    }
                }
            }
            """,
            "variables": {
                "input": {
                    "title": "Test Shout",
                    "body": "This is a test shout",
                }
            }
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "errors" not in data
    assert data["data"]["create_draft"]["draft"]["title"] == "Test Shout"

@pytest.mark.asyncio
async def test_load_drafts(test_client, db_session):
    """Test retrieving a shout."""
    response = test_client.post(
        "/",
        json={
            "query": """
            query {
                load_drafts {
                    error
                    drafts {
                        id
                        title
                        body
                    }
                }
            }
            """,
            "variables": {
                "slug": "test-shout"
            }
        }
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "errors" not in data
    assert data["data"]["load_drafts"]["drafts"] == [] 