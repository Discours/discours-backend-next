from typing import List

from sqlalchemy import and_, select

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor, ShoutReactionsFollower, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.reader import apply_filters, apply_sorting, get_shouts_with_links, has_field, query_with_stat
from services.auth import login_required
from services.db import local_session
from services.schema import query
from utils.logger import root_logger as logger


def apply_options(q, options, reactions_created_by=0):
    """
    Применяет опции фильтрации и сортировки
    [опционально] выбирая те публикации, на которые есть реакции от указанного автора

    :param q: Исходный запрос.
    :param options: Опции фильтрации и сортировки.
    :param reactions_created_by: Идентификатор автора.
    :return: Запрос с примененными опциями.
    """
    filters = options.get("filters")
    if isinstance(filters, dict):
        q = apply_filters(q, filters)
        if reactions_created_by:
            if "reacted" in filters or "commented" in filters:
                commented = filters.get("commented")
                reacted = filters.get("reacted") or commented
                q = q.join(Reaction, Reaction.shout == Shout.id)
                if commented:
                    q = q.filter(Reaction.body.is_not(None))
                if reacted:
                    q = q.filter(Reaction.created_by == reactions_created_by)
                else:
                    q = q.filter(Reaction.created_by != reactions_created_by)
    q = apply_sorting(q, options)
    limit = options.get("limit", 10)
    offset = options.get("offset", 0)
    return q, limit, offset


@query.field("load_shouts_coauthored")
@login_required
async def load_shouts_coauthored(_, info, options):
    """
    Загрузка публикаций, написанных в соавторстве с пользователем.

    :param info: Информаци о контексте GraphQL.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций в соавтостве.
    """
    author_id = info.context.get("author", {}).get("id")
    if not author_id:
        return []
    q = query_with_stat(info)
    q = q.filter(Shout.authors.any(id=author_id))
    q, limit, offset = apply_options(q, options)
    return get_shouts_with_links(info, q, limit, offset=offset)


@query.field("load_shouts_discussed")
@login_required
async def load_shouts_discussed(_, info, options):
    """
    Загрузка публикаций, которые обсуждались пользователем.

    :param info: Информация о контексте GraphQL.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций, обсужденых пользователем.
    """
    author_id = info.context.get("author", {}).get("id")
    if not author_id:
        return []
    q = query_with_stat(info)
    options["filters"]["commented"] = True
    q, limit, offset = apply_options(q, options, author_id)
    return get_shouts_with_links(info, q, limit, offset=offset)


def shouts_by_follower(info, follower_id: int, options):
    """
    Загружает публикации, на которые подписан автор.

    - по авторам
    - по темам
    - по реакциям

    :param info: Информация о контексте GraphQL.
    :param follower_id: Идентификатор автора.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций.
    """
    q = query_with_stat(info)
    reader_followed_authors = select(AuthorFollower.author).where(AuthorFollower.follower == follower_id)
    reader_followed_topics = select(TopicFollower.topic).where(TopicFollower.follower == follower_id)
    reader_followed_shouts = select(ShoutReactionsFollower.shout).where(ShoutReactionsFollower.follower == follower_id)

    followed_subquery = (
        select(Shout.id)
        .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
        .join(ShoutTopic, ShoutTopic.shout == Shout.id)
        .where(
            ShoutAuthor.author.in_(reader_followed_authors)
            | ShoutTopic.topic.in_(reader_followed_topics)
            | Shout.id.in_(reader_followed_shouts)
        )
    )
    q = q.filter(Shout.id.in_(followed_subquery))
    q, limit, offset = apply_options(q, options)
    shouts = get_shouts_with_links(info, q, limit, offset=offset)
    return shouts


@query.field("load_shouts_followed_by")
async def load_shouts_followed_by(_, info, slug: str, options) -> List[Shout]:
    """
    Загружает публикации, на которые подписан автор по slug.

    :param info: Информация о контексте GraphQL.
    :param slug: Slug автора.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций.
    """
    with local_session() as session:
        author = session.query(Author).filter(Author.slug == slug).first()
        if author:
            follower_id = author.dict()["id"]
            shouts = shouts_by_follower(info, follower_id, options)
            return shouts
    return []


@query.field("load_shouts_feed")
@login_required
async def load_shouts_feed(_, info, options) -> List[Shout]:
    """
    Загружает публикации, на которые подписан авторизованный пользователь.

    :param info: Информация о контексте GraphQL.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций.
    """
    author_id = info.context.get("author", {}).get("id")
    return shouts_by_follower(info, author_id, options) if author_id else []


@query.field("load_shouts_authored_by")
async def load_shouts_authored_by(_, info, slug: str, options) -> List[Shout]:
    """
    Загружает публикации, написанные автором по slug.
    """
    with local_session() as session:
        author = session.query(Author).filter(Author.slug == slug).first()
        if author:
            try:
                author_id: int = author.dict()["id"]
                q = (
                    query_with_stat(info)
                    if has_field(info, "stat")
                    else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
                )
                q = q.filter(Shout.authors.any(id=author_id))
                q, limit, offset = apply_options(q, options, author_id)
                shouts = get_shouts_with_links(info, q, limit, offset=offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []


@query.field("load_shouts_with_topic")
async def load_shouts_with_topic(_, info, slug: str, options) -> List[Shout]:
    """
    Загружает публикации, связанные с темой по slug.
    """
    with local_session() as session:
        topic = session.query(Topic).filter(Topic.slug == slug).first()
        if topic:
            try:
                topic_id: int = topic.dict()["id"]
                q = (
                    query_with_stat(info)
                    if has_field(info, "stat")
                    else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
                )
                q = q.filter(Shout.topics.any(id=topic_id))
                q, limit, offset = apply_options(q, options)
                shouts = get_shouts_with_links(info, q, limit, offset=offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []
