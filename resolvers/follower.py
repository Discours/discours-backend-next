from typing import List

from graphql import GraphQLError
from sqlalchemy import select
from sqlalchemy.sql import and_

from cache.cache import (
    cache_author,
    cache_topic,
    get_cached_follower_authors,
    get_cached_follower_topics,
)
from orm.author import Author, AuthorFollower
from orm.community import Community, CommunityFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.db import local_session
from services.notify import notify_follower
from services.schema import mutation, query
from utils.logger import root_logger as logger


@mutation.field("follow")
@login_required
async def follow(_, info, what, slug="", entity_id=0):
    logger.debug("Начало выполнения функции 'follow'")
    user_id = info.context.get("user_id")
    follower_dict = info.context.get("author")
    logger.debug(f"follower: {follower_dict}")

    if not user_id or not follower_dict:
        return GraphQLError("unauthorized")

    follower_id = follower_dict.get("id")
    logger.debug(f"follower_id: {follower_id}")

    entity_classes = {
        "AUTHOR": (Author, AuthorFollower, get_cached_follower_authors, cache_author),
        "TOPIC": (Topic, TopicFollower, get_cached_follower_topics, cache_topic),
        "COMMUNITY": (Community, CommunityFollower, None, None),  # Нет методов кэша для сообщества
        "SHOUT": (Shout, ShoutReactionsFollower, None, None),  # Нет методов кэша для shout
    }

    if what not in entity_classes:
        logger.error(f"Неверный тип для следования: {what}")
        return {"error": "invalid follow type"}

    entity_class, follower_class, get_cached_follows_method, cache_method = entity_classes[what]
    entity_type = what.lower()
    entity_dict = None

    try:
        logger.debug("Попытка получить сущность из базы данных")
        with local_session() as session:
            entity_query = select(entity_class).filter(entity_class.slug == slug)
            entities = get_with_stat(entity_query)
            [entity] = entities
            if not entity:
                logger.warning(f"{what.lower()} не найден по slug: {slug}")
                return {"error": f"{what.lower()} not found"}
            if not entity_id and entity:
                entity_id = entity.id
            entity_dict = entity.dict()
            logger.debug(f"entity_id: {entity_id}, entity_dict: {entity_dict}")

        if entity_id:
            logger.debug("Проверка существующей подписки")
            with local_session() as session:
                existing_sub = (
                    session.query(follower_class)
                    .filter(follower_class.follower == follower_id, getattr(follower_class, entity_type) == entity_id)
                    .first()
                )
                if existing_sub:
                    logger.info(f"Пользователь {follower_id} уже подписан на {what.lower()} с ID {entity_id}")
                else:
                    logger.debug("Добавление новой записи в базу данных")
                    sub = follower_class(follower=follower_id, **{entity_type: entity_id})
                    logger.debug(f"Создан объект подписки: {sub}")
                    session.add(sub)
                    session.commit()
                    logger.info(f"Пользователь {follower_id} подписался на {what.lower()} с ID {entity_id}")

            follows = None
            if cache_method:
                logger.debug("Обновление кэша")
                await cache_method(entity_dict)
            if get_cached_follows_method:
                logger.debug("Получение подписок из кэша")
                existing_follows = await get_cached_follows_method(follower_id)
                follows = [*existing_follows, entity_dict] if not existing_sub else existing_follows
                logger.debug("Обновлен список подписок")

            if what == "AUTHOR" and not existing_sub:
                logger.debug("Отправка уведомления автору о подписке")
                await notify_follower(follower=follower_dict, author_id=entity_id, action="follow")

    except Exception as exc:
        logger.exception("Произошла ошибка в функции 'follow'")
        return {"error": str(exc)}

    return {f"{what.lower()}s": follows}


@mutation.field("unfollow")
@login_required
async def unfollow(_, info, what, slug="", entity_id=0):
    logger.debug("Начало выполнения функции 'unfollow'")
    user_id = info.context.get("user_id")
    follower_dict = info.context.get("author")
    logger.debug(f"follower: {follower_dict}")

    if not user_id or not follower_dict:
        logger.warning("Неавторизованный доступ при попытке отписаться")
        return {"error": "unauthorized"}

    follower_id = follower_dict.get("id")
    logger.debug(f"follower_id: {follower_id}")

    entity_classes = {
        "AUTHOR": (Author, AuthorFollower, get_cached_follower_authors, cache_author),
        "TOPIC": (Topic, TopicFollower, get_cached_follower_topics, cache_topic),
        "COMMUNITY": (Community, CommunityFollower, None, None),  # Нет методов кэша для сообщества
        "SHOUT": (Shout, ShoutReactionsFollower, None, None),  # Нет методов кэша для shout
    }

    if what not in entity_classes:
        logger.error(f"Неверный тип для отписки: {what}")
        return {"error": "invalid unfollow type"}

    entity_class, follower_class, get_cached_follows_method, cache_method = entity_classes[what]
    entity_type = what.lower()
    follows = []
    error = None

    try:
        logger.debug("Попытка получить сущность из базы данных")
        with local_session() as session:
            entity = session.query(entity_class).filter(entity_class.slug == slug).first()
            logger.debug(f"Полученная сущность: {entity}")
            if not entity:
                logger.warning(f"{what.lower()} не найден по slug: {slug}")
                return {"error": f"{what.lower()} not found"}
            if entity and not entity_id:
                entity_id = entity.id
                logger.debug(f"entity_id: {entity_id}")

            sub = (
                session.query(follower_class)
                .filter(
                    and_(
                        getattr(follower_class, "follower") == follower_id,
                        getattr(follower_class, entity_type) == entity_id,
                    )
                )
                .first()
            )
            logger.debug(f"Найдена подписка для удаления: {sub}")
            if sub:
                session.delete(sub)
                session.commit()
                logger.info(f"Пользователь {follower_id} отписался от {what.lower()} с ID {entity_id}")

                if cache_method:
                    logger.debug("Обновление кэша после отписки")
                    await cache_method(entity.dict())
                if get_cached_follows_method:
                    logger.debug("Получение подписок из кэша")
                    existing_follows = await get_cached_follows_method(follower_id)
                    follows = filter(lambda x: x["id"] != entity_id, existing_follows)
                    logger.debug("Обновлен список подписок")

                if what == "AUTHOR":
                    logger.debug("Отправка уведомления автору об отписке")
                    await notify_follower(follower=follower_dict, author_id=entity_id, action="unfollow")

    except Exception as exc:
        logger.exception("Произошла ошибка в функции 'unfollow'")
        import traceback

        traceback.print_exc()
        return {"error": str(exc)}

    # logger.debug(f"Функция 'unfollow' завершена успешно с результатом: {entity_type}s={follows}, error={error}")
    return {f"{entity_type}s": follows, "error": error}


@query.field("get_shout_followers")
def get_shout_followers(_, _info, slug: str = "", shout_id: int | None = None) -> List[Author]:
    logger.debug("Начало выполнения функции 'get_shout_followers'")
    followers = []
    try:
        with local_session() as session:
            shout = None
            if slug:
                shout = session.query(Shout).filter(Shout.slug == slug).first()
                logger.debug(f"Найден shout по slug: {slug} -> {shout}")
            elif shout_id:
                shout = session.query(Shout).filter(Shout.id == shout_id).first()
                logger.debug(f"Найден shout по ID: {shout_id} -> {shout}")

            if shout:
                reactions = session.query(Reaction).filter(Reaction.shout == shout.id).all()
                logger.debug(f"Полученные реакции для shout ID {shout.id}: {reactions}")
                for r in reactions:
                    followers.append(r.created_by)
                    logger.debug(f"Добавлен follower: {r.created_by}")

    except Exception as _exc:
        import traceback

        traceback.print_exc()
        logger.exception("Произошла ошибка в функции 'get_shout_followers'")
        return []

    # logger.debug(f"Функция 'get_shout_followers' завершена с {len(followers)} подписчиками")
    return followers
