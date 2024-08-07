from typing import List
from sqlalchemy.orm import aliased, selectinload, joinedload
from sqlalchemy.sql import union
from sqlalchemy.sql.expression import (
    and_,
    asc,
    case,
    desc,
    distinct,
    func,
    nulls_last,
    select,
    text,
)
from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.topic import get_topics_random
from services.auth import login_required
from services.db import local_session
from utils.logger import root_logger as logger
from services.schema import query
from services.search import search_text
from services.viewed import ViewedStorage


def query_shouts():
    """
    Базовый запрос для получения публикаций с подзапросами статистики, авторов и тем.

    :return: Запрос для получения публикаций.
    """
    # Создаем алиасы для таблиц для избежания конфликтов имен
    aliased_reaction = aliased(Reaction)
    shout_author = aliased(ShoutAuthor)
    shout_topic = aliased(ShoutTopic)

    # Основной запрос с подзапросами для получения статистики, авторов и тем
    q = (
        select(
            Shout,
            func.count(case((aliased_reaction.body.is_not(None), 1))).label("comments_stat"),
            func.sum(
                case(
                    (aliased_reaction.kind == ReactionKind.LIKE.value, 1),
                    (aliased_reaction.kind == ReactionKind.DISLIKE.value, -1),
                    else_=0,
                )
            ).label("rating_stat"),
            func.max(aliased_reaction.created_at).label("last_reacted_at"),
            func.json_agg(
                func.json_build_object(
                    "id",
                    Author.id,
                    "name",
                    Author.name,
                    "slug",
                    Author.slug,
                    "pic",
                    Author.pic,
                )
            )
            .filter(Author.id.is_not(None))
            .label("authors"),
            func.json_agg(
                func.json_build_object(
                    "id",
                    Topic.id,
                    "title",
                    Topic.title,
                    "body",
                    Topic.body,
                    "slug",
                    Topic.slug,
                )
            )
            .filter(Topic.id.is_not(None))
            .label("topics"),
        )
        .outerjoin(aliased_reaction, aliased_reaction.shout == Shout.id)
        .outerjoin(shout_author, shout_author.shout == Shout.id)
        .outerjoin(Author, Author.id == shout_author.author)
        .outerjoin(shout_topic, shout_topic.shout == Shout.id)
        .outerjoin(Topic, Topic.id == shout_topic.topic)
        .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        .group_by(Shout.id, Author.id, Topic.id)
    )

    return q, aliased_reaction


def get_shouts_with_stats(q, limit, offset=0, author_id=None):
    """
    Получение публикаций со статистикой, и подзапросами авторов и тем.

    :param q: Запрос
    :param limit: Ограничение на количество результатов.
    :param offset: Смещение для пагинации.
    :return: Список публикаций с включенной статистикой.
    """
    # Основной запрос для получения публикаций и объединения их с подзапросами
    q = (
        q.options(
            selectinload(Shout.authors),  # Eagerly load authors
            selectinload(Shout.topics),  # Eagerly load topics
        )
        .limit(limit)
        .offset(offset)
    )

    # Выполнение запроса и обработка результатов
    with local_session() as session:
        results = session.execute(q, {"author_id": author_id}).all() if author_id else session.execute(q).all()

    # Формирование списка публикаций с их данными
    shouts = []
    for shout, comments_stat, rating_stat, last_reacted_at, authors, topics in results:
        shout.authors = authors or []
        shout.topics = topics or []
        shout.stat = {
            "viewed": ViewedStorage.get_shout(shout.id),
            "followers": 0,  # FIXME: implement followers_stat
            "rating": rating_stat or 0,
            "commented": comments_stat or 0,
            "last_reacted_at": last_reacted_at,
        }
        shouts.append(shout)

    return shouts


def filter_my(info, session, q):
    """
    Фильтрация публикаций, основанная на подписках пользователя.

    :param info: Информация о контексте GraphQL.
    :param session: Сессия базы данных.
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


def apply_filters(q, filters, author_id=None):
    """
    Применение фильтров к запросу.

    :param q: Исходный запрос.
    :param filters: Словарь фильтров.
    :param author_id: Идентификатор автора (опционально).
    :return: Запрос с примененными фильтрами.
    """
    if isinstance(filters, dict):
        if filters.get("reacted"):
            q = q.join(
                Reaction,
                and_(
                    Reaction.shout == Shout.id,
                    Reaction.created_by == author_id,
                ),
            )

        featured_filter = filters.get("featured", "")
        if featured_filter:
            q = q.filter(Shout.featured_at.is_not(None))
        elif "featured" in filters:
            q = q.filter(Shout.featured_at.is_(None))
        else:
            pass
        by_layouts = filters.get("layouts")
        if by_layouts and isinstance(by_layouts, list):
            q = q.filter(Shout.layout.in_(by_layouts))
        by_author = filters.get("author")
        if by_author:
            q = q.filter(Shout.authors.any(slug=by_author))
        by_topic = filters.get("topic")
        if by_topic:
            q = q.filter(Shout.topics.any(slug=by_topic))
        by_after = filters.get("after")
        if by_after:
            ts = int(by_after)
            q = q.filter(Shout.created_at > ts)

    return q


@query.field("get_shout")
async def get_shout(_, info, slug: str):
    """
    Получение публикации по slug.

    :param _: Корневой объект запроса (не используется).
    :param info: Информация о контексте GraphQL.
    :param slug: Уникальный идентификатор шута.
    :return: Данные шута с включенной статистикой.
    """
    try:
        with local_session() as session:
            q, aliased_reaction = query_shouts()
            q = q.filter(Shout.slug == slug)

            results = session.execute(q).first()
            if results:
                [shout, commented_stat, rating_stat, last_reaction_at, authors, topics] = results

                shout.stat = {
                    "viewed": ViewedStorage.get_shout(shout.id),
                    "commented": commented_stat,
                    "rating": rating_stat,
                    "last_reacted_at": last_reaction_at,
                }
                shout.authors = authors or []
                shout.topics = topics or []

                for author_caption in (
                    session.query(ShoutAuthor)
                    .join(Shout)
                    .where(
                        and_(
                            Shout.slug == slug,
                            Shout.published_at.is_not(None),
                            Shout.deleted_at.is_(None),
                        )
                    )
                ):
                    for author in shout.authors:
                        if author.id == author_caption.author:
                            author.caption = author_caption.caption
                main_topic = (
                    session.query(Topic.slug)
                    .join(
                        ShoutTopic,
                        and_(
                            ShoutTopic.topic == Topic.id,
                            ShoutTopic.shout == shout.id,
                            ShoutTopic.main.is_(True),
                        ),
                    )
                    .first()
                )

                if main_topic:
                    shout.main_topic = main_topic[0]
                return shout
    except Exception as _exc:
        import traceback

        logger.error(traceback.format_exc())


@query.field("load_shouts_by")
async def load_shouts_by(_, _info, options):
    """
    Загрузка публикаций с фильтрацией, сортировкой и пагинацией.

    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций, удовлетворяющих критериям.
    """
    # Базовый запрос
    q, aliased_reaction = query_shouts()

    # Применение фильтров
    filters = options.get("filters", {})
    q = apply_filters(q, filters)

    # Сортировка
    order_by = Shout.featured_at if filters.get("featured") else Shout.published_at
    order_str = options.get("order_by")
    if order_str in ["rating", "followers", "comments", "last_reacted_at"]:
        # TODO: implement followers_stat
        q = q.order_by(desc(text(f"{order_str}_stat")))
        query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
        q = q.order_by(nulls_last(query_order_by))
    else:
        q = q.order_by(Shout.published_at.desc().nulls_last())

    # Ограничение и смещение
    offset = options.get("offset", 0)
    limit = options.get("limit", 10)

    return get_shouts_with_stats(q, limit, offset)


@query.field("load_shouts_feed")
@login_required
async def load_shouts_feed(_, info, options):
    """
    Загрузка ленты публикаций для авторизованного пользователя.

    :param info: Информация о контексте GraphQL.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций для ленты.
    """
    with local_session() as session:
        q, aliased_reaction = query_shouts()

        # Применение фильтров
        filters = options.get("filters", {})
        if filters:
            q, reader_id = filter_my(info, session, q)
            q = apply_filters(q, filters, reader_id)

        # Сортировка
        order_by = options.get("order_by")
        order_by = text(order_by) if order_by else Shout.featured_at if filters.get("featured") else Shout.published_at
        query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
        q = q.order_by(nulls_last(query_order_by))

        # Пагинация
        offset = options.get("offset", 0)
        limit = options.get("limit", 10)

        return get_shouts_with_stats(q, limit, offset)


@query.field("load_shouts_search")
async def load_shouts_search(_, _info, text, limit=50, offset=0):
    """
    Поиск публикаций по тексту.

    :param text: Строка поиска.
    :param limit: Максимальное количество результатов.
    :param offset: Смещение для пагинации.
    :return: Список публикаций, найденных по тексту.
    """
    if isinstance(text, str) and len(text) > 2:
        results = await search_text(text, limit, offset)
        scores = {}
        hits_ids = []
        for sr in results:
            shout_id = sr.get("id")
            if shout_id:
                shout_id = str(shout_id)
                scores[shout_id] = sr.get("score")
                hits_ids.append(shout_id)

        q, aliased_reaction = query_shouts()
        q = q.filter(Shout.id.in_(hits_ids))
        shouts = get_shouts_with_stats(q, limit, offset)
        for shout in shouts:
            shout.score = scores[f"{shout.id}"]
            shouts.append(shout)
        shouts.sort(key=lambda x: x.score, reverse=True)
        return shouts
    return []


@query.field("load_shouts_unrated")
@login_required
async def load_shouts_unrated(_, info, limit: int = 50, offset: int = 0):
    """
    Загрузка публикаций с наименьшим количеством оценок.

    :param info: Информация о контексте GraphQL.
    :param limit: Максимальное количество результатов.
    :param offset: Смещение для пагинации.
    :return: Список публикаций с минимальным количеством оценок.
    """
    author_id = info.context.get("author", {}).get("id")
    if not author_id:
        return []
    q, aliased_reaction = query_shouts()

    q = (
        q.outerjoin(
            aliased_reaction,
            and_(
                aliased_reaction.shout == Shout.id,
                aliased_reaction.reply_to.is_(None),
                aliased_reaction.created_by != author_id,
                aliased_reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
            ),
        )
        .filter(Shout.deleted_at.is_(None))
        .filter(Shout.published_at.is_not(None))
    )

    q = q.having(func.count(distinct(aliased_reaction.id)) <= 4)  # 3 или менее голосов
    q = q.order_by(func.random())

    return get_shouts_with_stats(q, limit, offset=offset, author_id=author_id)


@query.field("load_shouts_random_top")
async def load_shouts_random_top(_, _info, options):
    """
    Загрузка случайных публикаций, упорядоченных по топовым реакциям.

    :param _info: Информация о контексте GraphQL.
    :param options: Опции фильтрации и сортировки.
    :return: Список случайных публикаций.
    """
    aliased_reaction = aliased(Reaction)

    subquery = (
        select(Shout.id).outerjoin(aliased_reaction).where(and_(Shout.deleted_at.is_(None), Shout.layout.is_not(None)))
    )

    subquery = apply_filters(subquery, options.get("filters", {}))

    subquery = subquery.group_by(Shout.id).order_by(
        desc(
            func.sum(
                case(
                    # не учитывать реакции на комментарии
                    (aliased_reaction.reply_to.is_not(None), 0),
                    (aliased_reaction.kind == ReactionKind.LIKE.value, 1),
                    (aliased_reaction.kind == ReactionKind.DISLIKE.value, -1),
                    else_=0,
                )
            )
        )
    )

    random_limit = options.get("random_limit", 100)
    if random_limit:
        subquery = subquery.limit(random_limit)
    q, aliased_reaction = query_shouts()
    q = q.filter(Shout.id.in_(subquery))
    q = q.order_by(func.random())
    limit = options.get("limit", 10)
    return get_shouts_with_stats(q, limit)


@query.field("load_shouts_random_topic")
async def load_shouts_random_topic(_, info, limit: int = 10):
    """
    Загрузка случайной темы и связанных с ней публикаций.

    :param info: Информация о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :return: Тема и связанные публикации.
    """
    [topic] = get_topics_random(None, None, 1)
    if topic:
        q, aliased_reaction = query_shouts()
        q = q.filter(Shout.topics.any(slug=topic.slug))
        q = q.order_by(desc(Shout.created_at))
        shouts = get_shouts_with_stats(q, limit)
        if shouts:
            return {"topic": topic, "shouts": shouts}
    return {"error": "failed to get random topic"}


@query.field("load_shouts_coauthored")
@login_required
async def load_shouts_coauthored(_, info, limit=50, offset=0):
    """
    Загрузка публикаций, написанных в соавторстве с пользователем.

    :param info: Информация о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :param offset: Смещение для пагинации.
    :return: Список публикаций в соавторстве.
    """
    author_id = info.context.get("author", {}).get("id")
    if not author_id:
        return []
    q, aliaed_reaction = query_shouts()
    q = q.filter(Shout.authors.any(id=author_id))
    return get_shouts_with_stats(q, limit, offset=offset)


@query.field("load_shouts_discussed")
@login_required
async def load_shouts_discussed(_, info, limit=50, offset=0):
    """
    Загрузка публикаций, которые обсуждались пользователем.

    :param info: Информация о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :param offset: Смещение для пагинации.
    :return: Список публикаций, обсужденных пользователем.
    """
    author_id = info.context.get("author", {}).get("id")
    if not author_id:
        return []
    # Subquery to find shout IDs that the author has commented on
    reaction_subquery = (
        select(Reaction.shout)
        .distinct()  # Ensure distinct shout IDs
        .filter(and_(Reaction.created_by == author_id, Reaction.body.is_not(None)))
        .correlate(Shout)  # Ensure proper correlation with the main query
    )
    q, aliased_reaction = query_shouts()
    q = q.filter(Shout.id.in_(reaction_subquery))
    return get_shouts_with_stats(q, limit, offset=offset)


async def reacted_shouts_updates(follower_id: int, limit=50, offset=0) -> List[Shout]:
    """
    Обновляет публикации, на которые подписан автор, с учетом реакций.

    :param follower_id: Идентификатор подписчика.
    :param limit: Количество публикаций для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список публикаций.
    """
    shouts: List[Shout] = []
    with local_session() as session:
        author = session.query(Author).filter(Author.id == follower_id).first()
        if author:
            # Публикации, где подписчик является автором
            q1, aliased_reaction1 = query_shouts()
            q1 = q1.filter(Shout.authors.any(id=follower_id))

            # Публикации, на которые подписчик реагировал
            q2, aliased_reaction2 = query_shouts()
            q2 = q2.options(joinedload(Shout.reactions))
            q2 = q2.filter(Reaction.created_by == follower_id)

            # Сортировка публикаций по полю `last_reacted_at`
            combined_query = union(q1, q2).order_by(desc(text("last_reacted_at")))

            # извлечение ожидаемой структуры данных
            shouts = get_shouts_with_stats(combined_query, limit, offset=offset)

    return shouts


@query.field("load_shouts_followed")
@login_required
async def load_shouts_followed(_, info, limit=50, offset=0) -> List[Shout]:
    """
    Загружает публикации, на которые подписан пользователь.

    :param info: Информация о контексте GraphQL.
    :param limit: Количество публикаций для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список публикаций.
    """
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            try:
                author_id: int = author.dict()["id"]
                shouts = await reacted_shouts_updates(author_id, limit, offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []


@query.field("load_shouts_followed_by")
async def load_shouts_followed_by(_, info, slug: str, limit=50, offset=0) -> List[Shout]:
    """
    Загружает публикации, на которые подписан автор по slug.

    :param info: Информация о контексте GraphQL.
    :param slug: Slug автора.
    :param limit: Количество публикаций для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список публикаций.
    """
    with local_session() as session:
        author = session.query(Author).filter(Author.slug == slug).first()
        if author:
            try:
                author_id: int = author.dict()["id"]
                shouts = await reacted_shouts_updates(author_id, limit, offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []
