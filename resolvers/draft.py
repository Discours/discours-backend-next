import time

from sqlalchemy.sql import and_

from cache.cache import (
    cache_author,
    cache_by_id,
    cache_topic,
    invalidate_shout_related_cache,
    invalidate_shouts_cache,
)
from orm.author import Author
from orm.draft import Draft
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from services.auth import login_required
from services.db import local_session
from services.notify import notify_shout
from services.schema import mutation, query
from services.search import search_service
from utils.logger import root_logger as logger


def create_shout_from_draft(session, draft, author_id):
    # Создаем новую публикацию
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
        draft=draft.id,
        deleted_at=None,
    )
    return shout


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
async def create_draft(_, info, draft_input):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    draft_id = draft_input.get("id")

    if not draft_id:
        return {"error": "Draft ID is required"}
    if not user_id or not author_id:
        return {"error": "Author ID are required"}

    with local_session() as session:
        draft = Draft(created_by=author_id, **draft_input)
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
    if not draft_id:
        return {"error": "Draft ID is required"}
    if not user_id or not author_id:
        return {"error": "Author ID are required"}

    with local_session() as session:
        draft = session.query(Draft).filter(Draft.id == draft_id).first()
        del draft_input["id"]
        Draft.update(draft, {**draft_input})
        if not draft:
            return {"error": "Draft not found"}

        draft.updated_at = int(time.time())
        session.commit()
        return {"draft": draft}


@mutation.field("delete_draft")
@login_required
async def delete_draft(_, info, draft_id: int):
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")

    with local_session() as session:
        draft = session.query(Draft).filter(Draft.id == draft_id).first()
        if not draft:
            return {"error": "Draft not found"}
        if author_id != draft.created_by and draft.authors.filter(Author.id == author_id).count() == 0:
            return {"error": "You are not allowed to delete this draft"}
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
        shout = create_shout_from_draft(session, draft, author_id)
        session.add(shout)
        session.commit()
        return {"shout": shout, "draft": draft}


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
        if not draft:
            return {"error": "Draft not found"}
        shout = session.query(Shout).filter(Shout.draft == draft.id).first()
        if shout:
            shout.published_at = None
            session.commit()
            return {"shout": shout, "draft": draft}
        return {"error": "Failed to unpublish draft"}


@mutation.field("publish_shout")
@login_required
async def publish_shout(_, info, shout_id: int):
    """Publish draft as a shout or update existing shout.

    Args:
        shout_id: ID существующей публикации или 0 для новой
        draft: Объект черновика (опционально)
    """
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    now = int(time.time())
    if not user_id or not author_id:
        return {"error": "User ID and author ID are required"}

    try:
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            if not shout:
                return {"error": "Shout not found"}
            was_published = shout.published_at is not None
            draft = session.query(Draft).where(Draft.id == shout.draft).first()
            if not draft:
                return {"error": "Draft not found"}
            # Находим черновик если не передан

            if not shout:
                shout = create_shout_from_draft(session, draft, author_id)
            else:
                # Обновляем существующую публикацию
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

                draft.updated_at = now
                shout.updated_at = now

                # Устанавливаем published_at только если была ранее снята с публикации
                if not was_published:
                    shout.published_at = now

            # Обрабатываем связи с авторами
            if (
                not session.query(ShoutAuthor)
                .filter(and_(ShoutAuthor.shout == shout.id, ShoutAuthor.author == author_id))
                .first()
            ):
                sa = ShoutAuthor(shout=shout.id, author=author_id)
                session.add(sa)

            # Обрабатываем темы
            if draft.topics:
                for topic in draft.topics:
                    st = ShoutTopic(
                        topic=topic.id, shout=shout.id, main=topic.main if hasattr(topic, "main") else False
                    )
                    session.add(st)

            session.add(shout)
            session.add(draft)
            session.flush()

            # Инвалидируем кэш только если это новая публикация или была снята с публикации
            if not was_published:
                cache_keys = ["feed", f"author_{author_id}", "random_top", "unrated"]

                # Добавляем ключи для тем
                for topic in shout.topics:
                    cache_keys.append(f"topic_{topic.id}")
                    cache_keys.append(f"topic_shouts_{topic.id}")
                    await cache_by_id(Topic, topic.id, cache_topic)

                # Инвалидируем кэш
                await invalidate_shouts_cache(cache_keys)
                await invalidate_shout_related_cache(shout, author_id)

                # Обновляем кэш авторов
                for author in shout.authors:
                    await cache_by_id(Author, author.id, cache_author)

                # Отправляем уведомление о публикации
                await notify_shout(shout.dict(), "published")

                # Обновляем поисковый индекс
                search_service.index(shout)
            else:
                # Для уже опубликованных материалов просто отправляем уведомление об обновлении
                await notify_shout(shout.dict(), "update")

            session.commit()
            return {"shout": shout}

    except Exception as e:
        logger.error(f"Failed to publish shout: {e}", exc_info=True)
        if "session" in locals():
            session.rollback()
        return {"error": f"Failed to publish shout: {str(e)}"}


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
