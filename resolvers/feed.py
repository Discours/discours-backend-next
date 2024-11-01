from typing import List

from sqlalchemy import and_, desc, select, text, union
from sqlalchemy.orm import joinedload

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor, ShoutReactionsFollower, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.reader import apply_filters, apply_sorting, get_shouts_with_links, has_field, query_with_stat
from services.auth import login_required
from services.db import local_session
from services.schema import query
from utils.logger import root_logger as logger


def apply_options(q, options, author_id: int):
    """
    Применяет опции фильтрации и сортировки к запросу для данного автора.

    :param q: Исходный запрос.
    :param options: Опции фильтрации и сортировки.
    :param author_id: Идентификатор автора.
    :return: Запрос с примененными опциями.
    """
    filters = options.get("filters")
    if isinstance(filters, dict):
        q = apply_filters(q, filters)
        if author_id and "reacted" in filters:
            reacted = filters.get("reacted")
            q = q.join(Reaction, Reaction.shout == Shout.id)
            if reacted:
                q = q.filter(Reaction.created_by == author_id)
            else:
                q = q.filter(Reaction.created_by != author_id)
    q = apply_sorting(q, options)
    limit = options.get("limit", 10)
    offset = options.get("offset", 0)
    return q, limit, offset


def filter_followed(info, q):
    """
    Фильтрация публикаций, основанная на подписках пользователя.

    :param info: Информация о контексте GraphQL.
    :param q: Исходный запрос для публикаций.
    :return: Фильтрованный запрос.
    """
    user_id = info.context.get("user_id")
    reader_id = info.context.get("author", {}).get("id")
    if user_id and reader_id:
        reader_followed_authors = select(AuthorFollower.author).where(AuthorFollower.follower == reader_id)
        reader_followed_topics = select(TopicFollower.topic).where(TopicFollower.follower == reader_id)
        reader_followed_shouts = select(ShoutReactionsFollower.shout).where(
            ShoutReactionsFollower.follower == reader_id
        )

        subquery = (
            select(Shout.id)
            .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
            .join(ShoutTopic, ShoutTopic.shout == Shout.id)
            .where(
                ShoutAuthor.author.in_(reader_followed_authors)
                | ShoutTopic.topic.in_(reader_followed_topics)
                | Shout.id.in_(reader_followed_shouts)
            )
        )
        q = q.filter(Shout.id.in_(subquery))
    return q, reader_id


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
    q = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    q = q.filter(Shout.authors.any(id=author_id))

    filters = options.get("filters")
    if isinstance(filters, dict):
        q = apply_filters(q, filters)
        if filters.get("reacted"):
            q = q.join(
                Reaction,
                and_(
                    Reaction.shout == Shout.id,
                    Reaction.created_by == author_id,
                ),
            )
    q = apply_sorting(q, options)
    limit = options.get("limit", 10)
    offset = options.get("offset", 0)
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
    # Подзапрос для поиска идентификаторов публикаций, которые комментировал автор
    reaction_subquery = (
        select(Reaction.shout)
        .distinct()  # Убедитесь, что получены уникальные идентификаторы публикаций
        .filter(and_(Reaction.created_by == author_id, Reaction.body.is_not(None)))
        .correlate(Shout)  # Убедитесь, что подзапрос правильно связан с основным запросом
    )
    q = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    q = q.filter(Shout.id.in_(reaction_subquery))
    q, limit, offset = apply_options(q, options, author_id)
    return get_shouts_with_links(info, q, limit, offset=offset)


def shouts_by_follower(info, follower_id: int, options):
    """
    Загружает публикации, на которые подписан автор.

    :param info: Информация о контексте GraphQL.
    :param follower_id: Идентификатор автора.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций.
    """
    # Публикации, где подписчик является автором
    q1 = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    q1 = q1.filter(Shout.authors.any(id=follower_id))

    # Публикации, на которые подписчик реагировал
    q2 = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    q2 = q2.options(joinedload(Shout.reactions))
    q2 = q2.filter(Reaction.created_by == follower_id)

    # Сортировка публикаций по полю `last_reacted_at`
    combined_query = union(q1, q2).order_by(desc(text("last_reacted_at")))

    # извлечение ожидаемой структуры данных
    q, limit, offset = apply_options(combined_query, options, follower_id)
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
                    query_with_stat()
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
                    query_with_stat()
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
