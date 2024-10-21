from graphql import GraphQLError
from sqlalchemy import delete, insert

from orm.author import AuthorBookmark
from orm.shout import Shout
from services.common_result import CommonResult
from services.db import local_session
from services.schema import mutation, query


@query.field("load_shouts_bookmarked")
def load_shouts_bookmarked(_, info, limit=50, offset=0):
    """
    Load bookmarked shouts for the authenticated user.

    Args:
        limit (int): Maximum number of shouts to return.
        offset (int): Number of shouts to skip.

    Returns:
        list: List of bookmarked shouts.
    """
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    if not author_id:
        raise GraphQLError("User not authenticated")
    result = []
    with local_session() as db:
        result = db.query(AuthorBookmark).where(AuthorBookmark.author == author_id).offset(offset).limit(limit).all()
    return result


@mutation.field("toggle_bookmark_shout")
def toggle_bookmark_shout(_, info, slug: str) -> CommonResult:
    """
    Toggle bookmark status for a specific shout.

    Args:
        slug (str): Unique identifier of the shout.

    Returns:
        CommonResult: Result of the operation with bookmark status.
    """
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    if not author_id:
        raise GraphQLError("User not authenticated")

    with local_session() as db:
        shout = db.query(Shout).filter(Shout.slug == slug).first()
        if not shout:
            raise GraphQLError("Shout not found")

        existing_bookmark = (
            db.query(AuthorBookmark)
            .filter(AuthorBookmark.author == author_id, AuthorBookmark.shout == shout.id)
            .first()
        )

        if existing_bookmark:
            db.execute(
                delete(AuthorBookmark).where(AuthorBookmark.author == author_id, AuthorBookmark.shout == shout.id)
            )
            result = False
        else:
            db.execute(insert(AuthorBookmark).values(author=author_id, shout=shout.id))
            result = True

        db.commit()
        return result
