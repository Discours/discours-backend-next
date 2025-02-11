from datetime import datetime

import pytest

from orm.author import Author
from orm.reaction import ReactionKind
from orm.shout import Shout


@pytest.fixture
def test_setup(db_session):
    """Set up test data."""
    now = int(datetime.now().timestamp())
    author = Author(name="Test Author", slug="test-author", user="test-user-id")
    db_session.add(author)
    db_session.flush()

    shout = Shout(
        title="Test Shout",
        slug="test-shout",
        created_by=author.id,
        body="This is a test shout",
        layout="article",
        lang="ru",
        community=1,
        created_at=now,
        updated_at=now,
    )
    db_session.add_all([author, shout])
    db_session.commit()
    return {"author": author, "shout": shout}


@pytest.mark.asyncio
async def test_create_reaction(test_client, db_session, test_setup):
    """Test creating a reaction on a shout."""
    response = test_client.post(
        "/",
        json={
            "query": """
            mutation CreateReaction($reaction: ReactionInput!) {
                create_reaction(reaction: $reaction) {
                    error
                    reaction {
                        id
                        kind
                        body
                        created_by {
                            name
                        }
                    }
                }
            }
            """,
            "variables": {
                "reaction": {"shout": test_setup["shout"].id, "kind": ReactionKind.LIKE.value, "body": "Great post!"}
            },
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert "error" not in data
    assert data["data"]["create_reaction"]["reaction"]["kind"] == ReactionKind.LIKE.value
