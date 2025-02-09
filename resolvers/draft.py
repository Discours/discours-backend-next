import time
from importlib import invalidate_caches

from sqlalchemy import select

from cache.cache import invalidate_shout_related_cache, invalidate_shouts_cache
from orm.author import Author
from orm.draft import Draft
from orm.shout import Shout
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from utils.logger import root_logger as logger


@query.field("load_drafts")
@login_required
async def load_drafts(_, info):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")

    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    with local_session() as session:
        drafts = session.query(Draft).filter(Draft.authors.any(Author.id == author_id)).all()
    return {"drafts": drafts}


@mutation.field("create_draft")
@login_required
async def create_draft(_, info, shout_id: int = 0):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")

    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    with local_session() as session:
        draft = Draft(created_by=author_id)
        if shout_id:
            draft.shout = shout_id
        session.add(draft)
        session.commit()
        return {"draft": draft}


@mutation.field("update_draft")
@login_required
async def update_draft(_, info, draft_input):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    draft_id = draft_input.get("id")
    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    with local_session() as session:
        draft = session.query(Draft).filter(Draft.id == draft_id).first()
        Draft.update(draft, {**draft_input})
        if not draft:
            return {"error": "Draft not found"}

        draft.updated_at = int(time.time())
        session.commit()
        return {"draft": draft}


@mutation.field("delete_draft")
@login_required
async def delete_draft(_, info, draft_id: int):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")

    with local_session() as session:
        draft = session.query(Draft).filter(Draft.id == draft_id).first()
        if not draft:
            return {"error": "Draft not found"}
        session.delete(draft)
        session.commit()
        return {"draft": draft}


@mutation.field("publish_draft")
@login_required
async def publish_draft(_, info, draft_id: int):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    with local_session() as session:
        draft = session.query(Draft).filter(Draft.id == draft_id).first()
        if not draft:
            return {"error": "Draft not found"}
        return publish_shout(None, None, draft.shout, draft)


@mutation.field("unpublish_draft")
@login_required
async def unpublish_draft(_, info, draft_id: int):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    with local_session() as session:
        draft = session.query(Draft).filter(Draft.id == draft_id).first()
        shout_id = draft.shout
        unpublish_shout(None, None, shout_id)


@mutation.field("publish_shout")
@login_required
async def publish_shout(_, info, shout_id: int, draft=None):
    """Publish draft as a shout or update existing shout.

    Args:
        session: SQLAlchemy session to use for database operations
    """
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    try:
        # Use proper SQLAlchemy query
        with local_session() as session:
            if not draft:
                find_draft_stmt = select(Draft).where(Draft.shout == shout_id)
                draft = session.execute(find_draft_stmt).scalar_one_or_none()

            now = int(time.time())

            if not shout:
                # Create new shout from draft
                shout = Shout(
                    body=draft.body,
                    slug=draft.slug,
                    cover=draft.cover,
                    cover_caption=draft.cover_caption,
                    lead=draft.lead,
                    description=draft.description,
                    title=draft.title,
                    subtitle=draft.subtitle,
                    layout=draft.layout,
                    media=draft.media,
                    lang=draft.lang,
                    seo=draft.seo,
                    created_by=author_id,
                    community=draft.community,
                    authors=draft.authors.copy(),  # Create copies of relationships
                    topics=draft.topics.copy(),
                    draft=draft.id,
                    deleted_at=None,
                )
            else:
                # Update existing shout
                shout.authors = draft.authors.copy()
                shout.topics = draft.topics.copy()
                shout.draft = draft.id
                shout.created_by = author_id
                shout.title = draft.title
                shout.subtitle = draft.subtitle
                shout.body = draft.body
                shout.cover = draft.cover
                shout.cover_caption = draft.cover_caption
                shout.lead = draft.lead
                shout.description = draft.description
                shout.layout = draft.layout
                shout.media = draft.media
                shout.lang = draft.lang
                shout.seo = draft.seo

            shout.updated_at = now
            shout.published_at = now
            draft.updated_at = now
            draft.published_at = now
            session.add(shout)
            session.add(draft)
            session.commit()

        invalidate_shout_related_cache(shout)
        invalidate_shouts_cache()
        return {"shout": shout}
    except Exception as e:
        import traceback

        logger.error(f"Failed to publish shout: {e}")
        logger.error(traceback.format_exc())
        session.rollback()
        return {"error": "Failed to publish shout"}


@mutation.field("unpublish_shout")
@login_required
async def unpublish_shout(_, info, shout_id: int):
    """Unpublish a shout.

    Args:
        shout_id: The ID of the shout to unpublish

    Returns:
        dict: The unpublished shout or an error message
    """
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    if not author_id:
        return {"error": "Author ID is required"}

    shout = None
    with local_session() as session:
        try:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            shout.published_at = None
            session.commit()
            invalidate_shout_related_cache(shout)
            invalidate_shouts_cache()

        except Exception:
            session.rollback()
            return {"error": "Failed to unpublish shout"}

    return {"shout": shout}
