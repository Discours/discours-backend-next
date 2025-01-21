import time

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.functions import coalesce

from cache.cache import cache_author, cache_topic, invalidate_shouts_cache
from orm.author import Author
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.follower import follow, unfollow
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.db import local_session
from services.notify import notify_shout
from services.schema import mutation, query
from services.search import search_service
from utils.logger import root_logger as logger


async def cache_by_id(entity, entity_id: int, cache_method):
    caching_query = select(entity).filter(entity.id == entity_id)
    result = get_with_stat(caching_query)
    if not result or not result[0]:
        logger.warning(f"{entity.__name__} with id {entity_id} not found")
        return
    x = result[0]
    d = x.dict()  # convert object to dictionary
    cache_method(d)
    return d


@query.field("get_my_shout")
@login_required
async def get_my_shout(_, info, shout_id: int):
    # logger.debug(info)
    user_id = info.context.get("user_id", "")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    roles = info.context.get("roles", [])
    shout = None
    if not user_id or not author_id:
        return {"error": "unauthorized", "shout": None}
    with local_session() as session:
        shout = (
            session.query(Shout)
            .filter(Shout.id == shout_id)
            .options(joinedload(Shout.authors), joinedload(Shout.topics))
            .filter(Shout.deleted_at.is_(None))
            .first()
        )
        if not shout:
            return {"error": "no shout found", "shout": None}

        logger.debug(f"got {len(shout.authors)} shout authors, created by {shout.created_by}")
        is_editor = "editor" in roles
        logger.debug(f'viewer is{'' if is_editor else ' not'} editor')
        is_creator = author_id == shout.created_by
        logger.debug(f'viewer is{'' if is_creator else ' not'} creator')
        is_author = bool(list(filter(lambda x: x.id == int(author_id), [x for x in shout.authors])))
        logger.debug(f'viewer is{'' if is_creator else ' not'} author')
        can_edit = is_editor or is_author or is_creator

        if not can_edit:
            return {"error": "forbidden", "shout": None}

        logger.debug("got shout editor with data")
        return {"error": None, "shout": shout}


@query.field("get_shouts_drafts")
@login_required
async def get_shouts_drafts(_, info):
    # user_id = info.context.get("user_id")
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    shouts = []
    with local_session() as session:
        if author_id:
            q = (
                select(Shout)
                .options(joinedload(Shout.authors), joinedload(Shout.topics))
                .filter(and_(Shout.deleted_at.is_(None), Shout.created_by == int(author_id)))
                .filter(Shout.published_at.is_(None))
                .order_by(desc(coalesce(Shout.updated_at, Shout.created_at)))
                .group_by(Shout.id)
            )
            shouts = [shout for [shout] in session.execute(q).unique()]
    return {"shouts": shouts}


@mutation.field("create_shout")
@login_required
async def create_shout(_, info, inp):
    logger.info(f"Starting create_shout with input: {inp}")
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author")
    logger.debug(f"Context user_id: {user_id}, author: {author_dict}")

    if not author_dict:
        logger.error("Author profile not found in context")
        return {"error": "author profile was not found"}

    author_id = author_dict.get("id")
    if user_id and author_id:
        try:
            with local_session() as session:
                author_id = int(author_id)
                current_time = int(time.time())
                slug = inp.get("slug") or f"draft-{current_time}"

                logger.info(f"Creating shout with slug: {slug}")

                # Создаем объект Shout напрямую, без промежуточного словаря
                new_shout = Shout(
                    title=inp.get("title", ""),
                    subtitle=inp.get("subtitle", ""),
                    lead=inp.get("lead", ""),
                    description=inp.get("description", ""),
                    body=inp.get("body", ""),
                    layout=inp.get("layout", "article"),
                    created_by=author_id,
                    slug=slug,
                    published_at=None,
                    community=1,
                    created_at=current_time
                )

                # Check for duplicate slug
                logger.debug(f"Checking for existing slug: {slug}")
                same_slug_shout = session.query(Shout).filter(Shout.slug == new_shout.slug).first()
                c = 1
                while same_slug_shout is not None:
                    logger.debug(f"Found duplicate slug, trying iteration {c}")
                    new_shout.slug = f"{slug}-{c}"
                    same_slug_shout = session.query(Shout).filter(Shout.slug == new_shout.slug).first()
                    c += 1

                try:
                    logger.info("Creating new shout object")
                    session.add(new_shout)
                    session.commit()
                    logger.info(f"Created shout with ID: {new_shout.id}")
                except Exception as e:
                    logger.error(f"Error creating shout object: {e}", exc_info=True)
                    return {"error": f"Database error: {str(e)}"}

                # Get created shout
                try:
                    logger.debug(f"Retrieving created shout with slug: {slug}")
                    shout = session.query(Shout).where(Shout.slug == slug).first()
                    if not shout:
                        logger.error("Created shout not found in database")
                        return {"error": "Shout creation failed - not found after commit"}
                except Exception as e:
                    logger.error(f"Error retrieving created shout: {e}", exc_info=True)
                    return {"error": f"Error retrieving created shout: {str(e)}"}

                # Link author
                try:
                    logger.debug(f"Linking author {author_id} to shout {shout.id}")
                    existing_sa = session.query(ShoutAuthor).filter_by(shout=shout.id, author=author_id).first()
                    if not existing_sa:
                        sa = ShoutAuthor(shout=shout.id, author=author_id)
                        session.add(sa)
                        logger.info(f"Added author {author_id} to shout {shout.id}")
                except Exception as e:
                    logger.error(f"Error linking author: {e}", exc_info=True)
                    return {"error": f"Error linking author: {str(e)}"}

                # Link topics
                try:
                    logger.debug(f"Linking topics: {inp.get('topics', [])}")
                    topics = session.query(Topic).filter(Topic.slug.in_(inp.get("topics", []))).all()
                    for topic in topics:
                        existing_st = session.query(ShoutTopic).filter_by(shout=shout.id, topic=topic.id).first()
                        if not existing_st:
                            t = ShoutTopic(topic=topic.id, shout=shout.id)
                            session.add(t)
                            logger.info(f"Added topic {topic.id} to shout {shout.id}")
                except Exception as e:
                    logger.error(f"Error linking topics: {e}", exc_info=True)
                    return {"error": f"Error linking topics: {str(e)}"}

                try:
                    session.commit()
                    logger.info("Final commit successful")
                except Exception as e:
                    logger.error(f"Error in final commit: {e}", exc_info=True)
                    return {"error": f"Error in final commit: {str(e)}"}

                try:
                    logger.debug("Following created shout")
                    await follow(None, info, "shout", shout.slug)
                except Exception as e:
                    logger.warning(f"Error following shout: {e}", exc_info=True)
                    # Don't return error as this is not critical

                # После успешного создания обновляем статистику автора
                try:
                    author = session.query(Author).filter(Author.id == author_id).first()
                    if author and author.stat:
                        author.stat["shouts"] = author.stat.get("shouts", 0) + 1
                        session.add(author)
                        session.commit()
                        await cache_author(author.dict())
                except Exception as e:
                    logger.warning(f"Error updating author stats: {e}", exc_info=True)
                    # Не возвращаем ошибку, так как это некритично

                # Преобразуем связанные сущности в словари
                try:
                    shout_dict = shout.dict()
                    shout_dict["authors"] = [author.dict() for author in shout.authors]
                    shout_dict["topics"] = [topic.dict() for topic in shout.topics]
                    logger.info(f"Successfully created shout {shout.id} with authors and topics as dicts")
                    return {"shout": shout_dict}
                except Exception as e:
                    logger.warning(f"Error converting related entities to dicts: {e}", exc_info=True)
                    # Если преобразование не удалось, возвращаем исходный объект
                    return {"shout": shout}

        except Exception as e:
            logger.error(f"Unexpected error in create_shout: {e}", exc_info=True)
            return {"error": f"Unexpected error: {str(e)}"}

    error_msg = "cant create shout" if user_id else "unauthorized"
    logger.error(f"Create shout failed: {error_msg}")
    return {"error": error_msg}


def patch_main_topic(session, main_topic, shout):
    with session.begin():
        shout = session.query(Shout).options(joinedload(Shout.topics)).filter(Shout.id == shout.id).first()
        if not shout:
            return
        old_main_topic = (
            session.query(ShoutTopic).filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.main.is_(True))).first()
        )

        main_topic = session.query(Topic).filter(Topic.slug == main_topic).first()

        if main_topic:
            new_main_topic = (
                session.query(ShoutTopic)
                .filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.topic == main_topic.id))
                .first()
            )

            if old_main_topic and new_main_topic and old_main_topic is not new_main_topic:
                ShoutTopic.update(old_main_topic, {"main": False})
                session.add(old_main_topic)

                ShoutTopic.update(new_main_topic, {"main": True})
                session.add(new_main_topic)


def patch_topics(session, shout, topics_input):
    new_topics_to_link = [Topic(**new_topic) for new_topic in topics_input if new_topic["id"] < 0]
    if new_topics_to_link:
        session.add_all(new_topics_to_link)
        session.commit()

    for new_topic_to_link in new_topics_to_link:
        created_unlinked_topic = ShoutTopic(shout=shout.id, topic=new_topic_to_link.id)
        session.add(created_unlinked_topic)

    existing_topics_input = [topic_input for topic_input in topics_input if topic_input.get("id", 0) > 0]
    existing_topic_to_link_ids = [
        existing_topic_input["id"]
        for existing_topic_input in existing_topics_input
        if existing_topic_input["id"] not in [topic.id for topic in shout.topics]
    ]

    for existing_topic_to_link_id in existing_topic_to_link_ids:
        created_unlinked_topic = ShoutTopic(shout=shout.id, topic=existing_topic_to_link_id)
        session.add(created_unlinked_topic)

    topic_to_unlink_ids = [
        topic.id
        for topic in shout.topics
        if topic.id not in [topic_input["id"] for topic_input in existing_topics_input]
    ]

    session.query(ShoutTopic).filter(
        and_(ShoutTopic.shout == shout.id, ShoutTopic.topic.in_(topic_to_unlink_ids))
    ).delete(synchronize_session=False)


@mutation.field("update_shout")
@login_required
async def update_shout(_, info, shout_id: int, shout_input=None, publish=False):
    user_id = info.context.get("user_id")
    roles = info.context.get("roles", [])
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    shout_input = shout_input or {}
    current_time = int(time.time())
    shout_id = shout_id or shout_input.get("id", shout_id)
    slug = shout_input.get("slug")
    if not user_id:
        return {"error": "unauthorized"}
    try:
        with local_session() as session:
            if author_id:
                logger.info(f"author for shout#{shout_id} detected author #{author_id}")
                shout_by_id = session.query(Shout).filter(Shout.id == shout_id).first()

                if not shout_by_id:
                    logger.error(f"shout#{shout_id} not found")
                    return {"error": "shout not found"}
                logger.info(f"shout#{shout_id} found")

                if slug != shout_by_id.slug:
                    same_slug_shout = session.query(Shout).filter(Shout.slug == slug).first()
                    c = 1
                    while same_slug_shout is not None:
                        c += 1
                        slug = f"{slug}-{c}"
                        same_slug_shout = session.query(Shout).filter(Shout.slug == slug).first()
                    shout_input["slug"] = slug
                    logger.info(f"shout#{shout_id} slug patched")

                if filter(lambda x: x.id == author_id, [x for x in shout_by_id.authors]) or "editor" in roles:
                    logger.info(f"shout#{shout_id} is author or editor")
                    # topics patch
                    topics_input = shout_input.get("topics")
                    if topics_input:
                        logger.info(f"topics_input: {topics_input}")
                        patch_topics(session, shout_by_id, topics_input)
                        del shout_input["topics"]
                        for tpc in topics_input:
                            await cache_by_id(Topic, tpc["id"], cache_topic)

                    # main topic
                    main_topic = shout_input.get("main_topic")
                    if main_topic:
                        patch_main_topic(session, main_topic, shout_by_id)

                    shout_input["updated_at"] = current_time
                    shout_input["published_at"] = current_time if publish else None
                    Shout.update(shout_by_id, shout_input)
                    session.add(shout_by_id)
                    session.commit()

                    shout_dict = shout_by_id.dict()
                    # Преобразуем связанные сущности в словари
                    try:
                        shout_dict["authors"] = [author.dict() for author in shout_by_id.authors]
                        shout_dict["topics"] = [topic.dict() for topic in shout_by_id.topics]
                    except Exception as e:
                        logger.warning(f"Error converting related entities to dicts: {e}", exc_info=True)

                    # Инвалидация кэша после обновления
                    try:
                        logger.info("Invalidating cache after shout update")

                        cache_keys = [
                            "feed",  # лента
                            f"author_{author_id}",  # публикации автора
                            "random_top",  # случайные топовые
                            "unrated",  # неоцененные
                        ]

                        # Добавляем ключи для старых тем (до обновления)
                        for topic in shout_by_id.topics:
                            cache_keys.append(f"topic_{topic.id}")
                            cache_keys.append(f"topic_shouts_{topic.id}")

                        # Добавляем ключи для новых тем (если есть в shout_input)
                        if shout_input.get("topics"):
                            for topic in shout_input["topics"]:
                                if topic.get("id"):
                                    cache_keys.append(f"topic_{topic.id}")
                                    cache_keys.append(f"topic_shouts_{topic.id}")

                        await invalidate_shouts_cache(cache_keys)

                        # Обновляем кэш тем и авторов
                        for topic in shout_by_id.topics:
                            await cache_by_id(Topic, topic.id, cache_topic)
                        for author in shout_by_id.authors:
                            await cache_author(author.dict())

                        logger.info("Cache invalidated successfully")
                    except Exception as cache_error:
                        logger.warning(f"Cache invalidation error: {cache_error}", exc_info=True)

                    if not publish:
                        await notify_shout(shout_dict, "update")
                    else:
                        await notify_shout(shout_dict, "published")
                        # search service indexing
                        search_service.index(shout_by_id)
                        for a in shout_by_id.authors:
                            await cache_by_id(Author, a.id, cache_author)
                    logger.info(f"shout#{shout_id} updated")
                    return {"shout": shout_dict, "error": None}
                else:
                    logger.warning(f"shout#{shout_id} is not author or editor")
                    return {"error": "access denied", "shout": None}

    except Exception as exc:
        import traceback

        traceback.print_exc()
        logger.error(exc)
        logger.error(f" cannot update with data: {shout_input}")

    return {"error": "cant update shout"}


@mutation.field("delete_shout")
@login_required
async def delete_shout(_, info, shout_id: int):
    user_id = info.context.get("user_id")
    roles = info.context.get("roles", [])
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    if user_id and author_id:
        author_id = int(author_id)
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            if not isinstance(shout, Shout):
                return {"error": "invalid shout id"}
            shout_dict = shout.dict()
            # NOTE: only owner and editor can mark the shout as deleted
            if shout_dict["created_by"] == author_id or "editor" in roles:
                shout_dict["deleted_at"] = int(time.time())
                Shout.update(shout, shout_dict)
                session.add(shout)
                session.commit()

                for author in shout.authors:
                    await cache_by_id(Author, author.id, cache_author)
                    info.context["author"] = author.dict()
                    info.context["user_id"] = author.user
                    unfollow(None, info, "shout", shout.slug)

                for topic in shout.topics:
                    await cache_by_id(Topic, topic.id, cache_topic)

                await notify_shout(shout_dict, "delete")
                return {"error": None}
            else:
                return {"error": "access denied"}
