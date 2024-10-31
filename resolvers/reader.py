from typing import List

from sqlalchemy.orm import aliased, joinedload
from sqlalchemy.sql import union
from sqlalchemy.sql.expression import (
    and_,
    asc,
    case,
    desc,
    func,
    nulls_last,
    select,
    text,
)

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutReactionsFollower, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.topic import get_topics_random
from services.auth import login_required
from services.db import local_session
from services.schema import query
from services.search import search_text
from services.viewed import ViewedStorage
from utils.logger import root_logger as logger


def has_field(info, fieldname: str) -> bool:
    """Проверяет, запрошено ли поле :fieldname: в GraphQL запросе"""
    field_node = info.field_nodes[0]
    for selection in field_node.selection_set.selections:
        if hasattr(selection, "name") and selection.name.value == fieldname:
            return True
    return False


def query_with_stat():
    """
    Оптимизированный базовый запрос
    """
    # Оптимизированный подзапрос статистики
    stats_subquery = (
        select(
            Reaction.shout.label("shout_id"),
            func.count(case((Reaction.kind == ReactionKind.COMMENT.value, 1), else_=None)).label("comments_count"),
            func.sum(
                case(
                    (Reaction.kind == ReactionKind.LIKE.value, 1),
                    (Reaction.kind == ReactionKind.DISLIKE.value, -1),
                    else_=0,
                )
            ).label("rating"),
            func.max(case((Reaction.reply_to.is_(None), Reaction.created_at), else_=None)).label("last_reacted_at"),
        )
        .where(Reaction.deleted_at.is_(None))
        .group_by(Reaction.shout)
        .subquery()
    )

    q = (
        select(Shout, stats_subquery)
        .outerjoin(stats_subquery, stats_subquery.c.shout_id == Shout.id)
        .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    return q


def get_shouts_with_links(info, q, limit=20, offset=0, author_id=None):
    """
    Оптимизированное получение данных
    """
    if author_id:
        q = q.filter(Shout.created_by == author_id)

    if limit:
        q = q.limit(limit)
    if offset:
        q = q.offset(offset)

    with local_session() as session:
        # 1. Получаем шауты одним запросом
        shouts_result = session.execute(q).all()
        if not shouts_result:
            return []

        # 2. Получаем авторов и топики пакетным запросом
        shout_ids = [row.Shout.id for row in shouts_result]
        if has_field(info, "authors") or has_field(info, "topics"):
            authors_and_topics = session.execute(
                select(
                    ShoutAuthor.shout.label("shout_id"),
                    Author.id.label("author_id"),
                    Author.name.label("author_name"),
                    Author.slug.label("author_slug"),
                    Author.pic.label("author_pic"),
                    ShoutAuthor.caption.label("author_caption"),
                    Topic.id.label("topic_id"),
                    Topic.title.label("topic_title"),
                    Topic.slug.label("topic_slug"),
                    ShoutTopic.main.label("topic_is_main"),
                )
                .outerjoin(Author, ShoutAuthor.author == Author.id)
                .outerjoin(ShoutTopic, ShoutTopic.shout == ShoutAuthor.shout)
                .outerjoin(Topic, ShoutTopic.topic == Topic.id)
                .where(ShoutAuthor.shout.in_(shout_ids))
            ).all()

        # 3. Группируем данные эффективно
        shouts_data = {}
        for row in shouts_result:
            shout = row.Shout
            shout_id = shout.id
            shout_dict = shout.dict()

            # Добавляем статистику только если она запрошена
            if has_field(info, "stat"):
                viewed_stat = ViewedStorage.get_shout(shout_id=shout_id) or 0
                shout_dict["stat"] = {
                    "viewed": viewed_stat,
                    "commented": row.comments_count or 0,
                    "rating": row.rating or 0,
                    "last_reacted_at": row.last_reacted_at,
                }

            # Инициализируем списки только для запрошенных полей
            if has_field(info, "authors"):
                shout_dict["authors"] = []
            if has_field(info, "topics"):
                shout_dict["topics"] = set()  # используем set для уникальности

            shouts_data[shout_id] = shout_dict

        # 4. Заполняем связанные данные
        for row in authors_and_topics:
            shout_data = shouts_data[row.shout_id]

            # Добавляем автора
            author = {
                "id": row.author_id,
                "name": row.author_name,
                "slug": row.author_slug,
                "pic": row.author_pic,
                "caption": row.author_caption,
            }
            if author not in shout_data["authors"]:
                shout_data["authors"].append(author)

            # Добавляем топик если есть
            if row.topic_id:
                topic = {
                    "id": row.topic_id,
                    "title": row.topic_title,
                    "slug": row.topic_slug,
                    "is_main": row.topic_is_main,
                }
                shout_data["topics"].add(tuple(topic.items()))

        # 5. Финальная обработка и сортировка
        result = []
        for shout_data in shouts_data.values():
            # Конвертируем topics обратно в список словарей и сортируем
            shout_data["topics"] = sorted(
                [dict(t) for t in shout_data["topics"]], key=lambda x: (not x["is_main"], x["id"])
            )
            result.append(shout_data)

        return result


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

        if "featured" in filters:
            featured_filter = filters.get("featured")
            if featured_filter:
                q = q.filter(Shout.featured_at.is_not(None))
            else:
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
async def get_shout(_, info, slug="", shout_id=0):
    """
    Получение публикации по slug или id.

    :param _: Корневой объект запроса (не используется)
    :param _info: Информация о контексте GraphQL
    :param slug: Уникальный идентификатор публикации
    :param shout_id: ID публикации
    :return: Данные публикации с включенной статистикой
    """
    try:
        # Получаем базовый запрос с подзапросами статистики
        q = query_with_stat()

        # Применяем фильтр по slug или id
        if slug:
            q = q.where(Shout.slug == slug)
        elif shout_id:
            q = q.where(Shout.id == shout_id)
        else:
            return None

        # Получаем результат через get_shouts_with_stats с limit=1
        shouts = get_shouts_with_links(info, q, limit=1)

        # Возвращаем первую (и единственную) публикацию, если она найдена
        return shouts[0] if shouts else None

    except Exception as exc:
        logger.error(f"Error in get_shout: {exc}", exc_info=True)
        return None


@query.field("load_shouts_by")
async def load_shouts_by(_, info, options):
    """
    Загрузка публикаций с фильтрацией, сортировкой и пагинацией.

    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций, удовлетворяющих критериям.
    """
    # Базовый запрос
    q = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )

    # Применение фильтров
    filters = options.get("filters", {})
    q = apply_filters(q, filters)

    # Сортировка
    order_by = Shout.featured_at if filters.get("featured") else Shout.published_at
    order_str = options.get("order_by")
    if order_str in ["rating", "followers", "comments", "last_reacted_at"]:
        q = q.order_by(desc(text(f"{order_str}_stat")))
        query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
        q = q.order_by(nulls_last(query_order_by))
    else:
        q = q.order_by(Shout.published_at.desc())

    # Ограничение и смещение
    offset = options.get("offset", 0)
    limit = options.get("limit", 10)

    return get_shouts_with_links(info, q, limit, offset)


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
        q = (
            query_with_stat()
            if has_field(info, "stat")
            else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        )

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

        return get_shouts_with_links(info, q, limit, offset)


@query.field("load_shouts_search")
async def load_shouts_search(_, info, text, limit=50, offset=0):
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

        q = (
            query_with_stat()
            if has_field(info, "stat")
            else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        )
        q = q.filter(Shout.id.in_(hits_ids))
        shouts = get_shouts_with_links(info, q, limit, offset)
        for shout in shouts:
            shout.score = scores[f"{shout.id}"]
        shouts.sort(key=lambda x: x.score, reverse=True)
        return shouts
    return []


@query.field("load_shouts_unrated")
async def load_shouts_unrated(_, info, limit=50, offset=0):
    """
    Загрузка публикаций с менее чем 3 реакциями типа LIKE/DISLIKE
    """
    rated_shouts = (
        select(Reaction.shout)
        .where(
            and_(
                Reaction.deleted_at.is_(None), Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value])
            )
        )
        .group_by(Reaction.shout)
        .having(func.count("*") >= 3)
        .scalar_subquery()
    )

    q = (
        select(Shout)
        .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None), ~Shout.id.in_(rated_shouts)))
        .order_by(desc(Shout.published_at))
    )

    return get_shouts_with_links(info, q, limit, offset)


@query.field("load_shouts_random_top")
async def load_shouts_random_top(_, info, options):
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
    q = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    q = q.filter(Shout.id.in_(subquery))
    q = q.order_by(func.random())
    limit = options.get("limit", 10)
    return get_shouts_with_links(info, q, limit)


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
        q = (
            query_with_stat()
            if has_field(info, "stat")
            else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        )
        q = q.filter(Shout.topics.any(slug=topic.slug))
        q = q.order_by(desc(Shout.created_at))
        shouts = get_shouts_with_links(info, q, limit)
        if shouts:
            return {"topic": topic, "shouts": shouts}
    return {"error": "failed to get random topic"}


@query.field("load_shouts_coauthored")
@login_required
async def load_shouts_coauthored(_, info, limit=50, offset=0):
    """
    Загрузка публикаций, написанных в соавторстве с пользователем.

    :param info: Информаци о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :param offset: Смещение для пагинации.
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
    return get_shouts_with_links(info, q, limit, offset=offset)


@query.field("load_shouts_discussed")
@login_required
async def load_shouts_discussed(_, info, limit=50, offset=0):
    """
    Загрузка публикаций, которые обсуждались пользователем.

    :param info: Информация о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :param offset: Смещне для пагинации.
    :return: Список публикаций, обсужде��ных пользователем.
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
    return get_shouts_with_links(info, q, limit, offset=offset)


async def reacted_shouts_updates(info, follower_id: int, limit=50, offset=0) -> List[Shout]:
    """
    Обновляет публикации, на которые подписан автор, с учетом реакци.

    :param follower_id: Идентификатор подписчика.
    :param limit: Коли��ество пу��ликаций для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список публикаций.
    """
    shouts: List[Shout] = []
    with local_session() as session:
        author = session.query(Author).filter(Author.id == follower_id).first()
        if author:
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
            shouts = get_shouts_with_links(info, combined_query, limit, offset=offset)

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
                shouts = await reacted_shouts_updates(info, author_id, limit, offset)
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
                shouts = await reacted_shouts_updates(info, author_id, limit, offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []
