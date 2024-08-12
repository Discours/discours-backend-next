from sqlalchemy import distinct, func, select

from cache.cache import (
    get_cached_topic_authors,
    get_cached_topic_by_slug,
    get_cached_topic_followers,
)
from cache.memorycache import cache_region
from orm.author import Author
from orm.shout import ShoutTopic
from orm.topic import Topic
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from utils.logger import root_logger as logger


# Запрос на получение всех тем
@query.field("get_topics_all")
def get_topics_all(_, _info):
    cache_key = "get_topics_all"  # Ключ для кеша

    @cache_region.cache_on_arguments(cache_key)
    def _get_topics_all():
        topics_query = select(Topic)
        return get_with_stat(topics_query)  # Получение тем с учетом статистики

    return _get_topics_all()


# Запрос на получение тем по сообществу
@query.field("get_topics_by_community")
def get_topics_by_community(_, _info, community_id: int):
    cache_key = f"get_topics_by_community_{community_id}"  # Ключ для кеша

    @cache_region.cache_on_arguments(cache_key)
    def _get_topics_by_community():
        topics_by_community_query = select(Topic).where(Topic.community == community_id)
        return get_with_stat(topics_by_community_query)

    return _get_topics_by_community()


# Запрос на получение тем по автору
@query.field("get_topics_by_author")
async def get_topics_by_author(_, _info, author_id=0, slug="", user=""):
    topics_by_author_query = select(Topic)
    if author_id:
        topics_by_author_query = topics_by_author_query.join(Author).where(Author.id == author_id)
    elif slug:
        topics_by_author_query = topics_by_author_query.join(Author).where(Author.slug == slug)
    elif user:
        topics_by_author_query = topics_by_author_query.join(Author).where(Author.user == user)

    return get_with_stat(topics_by_author_query)


# Запрос на получение одной темы по её slug
@query.field("get_topic")
async def get_topic(_, _info, slug: str):
    topic = await get_cached_topic_by_slug(slug, get_with_stat)
    if topic:
        return topic


# Мутация для создания новой темы
@mutation.field("create_topic")
@login_required
async def create_topic(_, _info, inp):
    with local_session() as session:
        # TODO: проверить права пользователя на создание темы для конкретного сообщества
        # и разрешение на создание
        new_topic = Topic(**inp)
        session.add(new_topic)
        session.commit()

        return {"topic": new_topic}


# Мутация для обновления темы
@mutation.field("update_topic")
@login_required
async def update_topic(_, _info, inp):
    slug = inp["slug"]
    with local_session() as session:
        topic = session.query(Topic).filter(Topic.slug == slug).first()
        if not topic:
            return {"error": "topic not found"}
        else:
            Topic.update(topic, inp)
            session.add(topic)
            session.commit()

            return {"topic": topic}


# Мутация для удаления темы
@mutation.field("delete_topic")
@login_required
async def delete_topic(_, info, slug: str):
    user_id = info.context["user_id"]
    with local_session() as session:
        t: Topic = session.query(Topic).filter(Topic.slug == slug).first()
        if not t:
            return {"error": "invalid topic slug"}
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            if t.created_by != author.id:
                return {"error": "access denied"}

            session.delete(t)
            session.commit()

            return {}
    return {"error": "access denied"}


# Запрос на получение случайных тем
@query.field("get_topics_random")
def get_topics_random(_, _info, amount=12):
    q = select(Topic)
    q = q.join(ShoutTopic)
    q = q.group_by(Topic.id)
    q = q.having(func.count(distinct(ShoutTopic.shout)) > 2)
    q = q.order_by(func.random()).limit(amount)

    topics = []
    with local_session() as session:
        for [topic] in session.execute(q):
            topics.append(topic)

    return topics


# Запрос на получение подписчиков темы
@query.field("get_topic_followers")
async def get_topic_followers(_, _info, slug: str):
    logger.debug(f"getting followers for @{slug}")
    topic = await get_cached_topic_by_slug(slug, get_with_stat)
    topic_id = topic.id if isinstance(topic, Topic) else topic.get("id")
    followers = await get_cached_topic_followers(topic_id)
    return followers


# Запрос на получение авторов темы
@query.field("get_topic_authors")
async def get_topic_authors(_, _info, slug: str):
    logger.debug(f"getting authors for @{slug}")
    topic = await get_cached_topic_by_slug(slug, get_with_stat)
    topic_id = topic.id if isinstance(topic, Topic) else topic.get("id")
    authors = await get_cached_topic_authors(topic_id)
    return authors
