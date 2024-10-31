from typing import List

from sqlalchemy.orm import aliased, joinedload
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
from orm.shout import Shout, ShoutAuthor, ShoutReactionsFollower, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.topic import get_topics_random
from services.auth import login_required
from services.db import local_session
from services.schema import query
from services.search import search_text
from services.viewed import ViewedStorage
from utils.logger import root_logger as logger


def query_shouts():
    """
    Базовый запрос для получения публикаций с подзапросами статистики, авторов и тем.
    """
    # Подзапрос для реакций и статистики (объединяем только эту часть)
    reactions_subquery = (
        select(
            func.sum(
                case(
                    (Reaction.kind == ReactionKind.LIKE.value, 1),
                    (Reaction.kind == ReactionKind.DISLIKE.value, -1),
                    else_=0,
                )
            ).label("rating_stat"),
            func.count(distinct(case((Reaction.kind == ReactionKind.COMMENT.value, Reaction.id), else_=None))).label(
                "comments_stat"
            ),
            func.max(Reaction.created_at).label("last_reacted_at"),
        )
        .select_from(Reaction)
        .where(and_(Reaction.shout == Shout.id, Reaction.reply_to.is_(None), Reaction.deleted_at.is_(None)))
        .correlate(Shout)
        .scalar_subquery()
    )

    # Остальные подзапросы оставляем как есть
    authors_subquery = (
        select(
            func.json_agg(
                func.json_build_object("id", Author.id, "name", Author.name, "slug", Author.slug, "pic", Author.pic)
            ).label("authors")
        )
        .select_from(ShoutAuthor)
        .join(Author, ShoutAuthor.author == Author.id)
        .where(ShoutAuthor.shout == Shout.id)
        .correlate(Shout)
        .scalar_subquery()
    )

    # Подзапрос для уникальных тем, агрегированных в JSON
    topics_subquery = (
        select(
            func.json_agg(func.json_build_object("id", Topic.id, "title", Topic.title, "slug", Topic.slug)).label(
                "topics"
            )
        )
        .select_from(ShoutTopic)
        .join(Topic, ShoutTopic.topic == Topic.id)
        .where(ShoutTopic.shout == Shout.id)
        .correlate(Shout)
        .scalar_subquery()
    )

    # Новый подзапрос для main_topic_slug
    main_topic_subquery = (
        select(func.max(Topic.slug).label("main_topic_slug"))
        .select_from(ShoutTopic)
        .join(Topic, ShoutTopic.topic == Topic.id)
        .where(and_(ShoutTopic.shout == Shout.id, ShoutTopic.main.is_(True)))
        .correlate(Shout)
        .scalar_subquery()
    )

    captions_subquery = (
        select(
            func.json_agg(func.json_build_object("author_id", Author.id, "caption", ShoutAuthor.caption)).label(
                "captions"
            )
        )
        .select_from(ShoutAuthor)
        .join(Author, ShoutAuthor.author == Author.id)
        .where(ShoutAuthor.shout == Shout.id)
        .correlate(Shout)
        .scalar_subquery()
    )

    # Основной запрос
    q = (
        select(
            Shout,
            reactions_subquery,
            authors_subquery,
            captions_subquery,
            topics_subquery,
            main_topic_subquery,
        )
        .outerjoin(Reaction, Reaction.shout == Shout.id)
        .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        .group_by(Shout.id)
    )

    return q


def get_shouts_with_stats(q, limit=20, offset=0, author_id=None):
    """
    Получение публикаций со статистикой.

    :param q: Базовый запрос публикаций
    :param limit: Ограничение количества результатов
    :param offset: Смещение для пагинации
    :param author_id: Опциональный ID автора для фильтрации
    :return: Список публикаций с статистикой
    """
    if author_id:
        q = q.filter(Shout.created_by == author_id)

    q = q.order_by(Shout.published_at.desc().nulls_last())

    if limit:
        q = q.limit(limit)
    if offset:
        q = q.offset(offset)

    shouts = []
    with local_session() as session:
        results = session.execute(q).all()

        for [shout, reactions_stat, authors_json, captions_json, topics_json, main_topic_slug] in results:
            # Базовые данные публикации
            shout_dict = shout.dict()

            # Добавление статистики просмотров
            viewed_stat = ViewedStorage.get_shout(shout_slug=shout.slug)

            # Обработка авторов и их подписей
            authors = authors_json or []
            captions = captions_json or []

            # Объединяем авторов с их подписями
            for author in authors:
                caption_item = next((c for c in captions if c["author_id"] == author["id"]), None)
                if caption_item:
                    author["caption"] = caption_item["caption"]

            # Обработка тем
            topics = topics_json or []
            for topic in topics:
                topic["is_main"] = topic["slug"] == main_topic_slug

            # Формирование финальной структуры
            shout_dict.update(
                {
                    "authors": authors,
                    "topics": topics,
                    "main_topic": main_topic_slug,
                    "stat": {
                        "viewed": viewed_stat or 0,
                        "rating": reactions_stat.rating_stat or 0,
                        "commented": reactions_stat.comments_stat or 0,
                        "last_reacted_at": reactions_stat.last_reacted_at,
                    },
                }
            )

            shouts.append(shout_dict)

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
async def get_shout(_, _info, slug="", shout_id=0):
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
        q = query_shouts()

        # Применяем фильтр по slug или id
        if slug:
            q = q.where(Shout.slug == slug)
        elif shout_id:
            q = q.where(Shout.id == shout_id)
        else:
            return None

        # Получаем результат через get_shouts_with_stats с limit=1
        shouts = get_shouts_with_stats(q, limit=1)

        # Возвращаем первую (и единственную) публикацию, если она найдена
        return shouts[0] if shouts else None

    except Exception as exc:
        logger.error(f"Error in get_shout: {exc}", exc_info=True)
        return None


@query.field("load_shouts_by")
async def load_shouts_by(_, _info, options):
    """
    Загрузка публикаций с фильтрацией, сортировкой и пагинацией.

    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций, удовлетворяющих критериям.
    """
    # Базовый запрос
    q = query_shouts()

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
        q = query_shouts()

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

        q = query_shouts()
        q = q.filter(Shout.id.in_(hits_ids))
        shouts = get_shouts_with_stats(q, limit, offset)
        for shout in shouts:
            shout.score = scores[f"{shout.id}"]
        shouts.sort(key=lambda x: x.score, reverse=True)
        return shouts
    return []


@query.field("load_shouts_unrated")
async def load_shouts_unrated(_, info, limit: int = 50, offset: int = 0):
    """
    Загрузка публикаций с наименьшим количеством оценок.
    """
    rating_reaction = aliased(Reaction, name="rating_reaction")

    # Подзапрос для подсчета количества оценок (лайков и дизлайков)
    ratings_count = (
        select(func.count(distinct(rating_reaction.id)))
        .select_from(rating_reaction)
        .where(
            and_(
                rating_reaction.shout == Shout.id,
                rating_reaction.reply_to.is_(None),
                rating_reaction.deleted_at.is_(None),
                rating_reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
            )
        )
        .correlate(Shout)
        .scalar_subquery()
        .label("ratings_count")
    )

    q = query_shouts()

    # Добавляем подсчет рейтингов в основной запрос
    q = q.add_columns(ratings_count)

    # Фильтруем только опубликованные и не удаленные публикации
    q = q.filter(and_(Shout.deleted_at.is_(None), Shout.published_at.is_not(None)))

    # Сортируем по количеству оценок (по возрастанию) и случайно среди равных
    q = q.order_by(ratings_count.asc(), func.random())

    return get_shouts_with_stats(q, limit, offset=offset)


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
    q = query_shouts()
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
        q = query_shouts()
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

    :param info: Информаци�� о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :param offset: Смещение для пагинации.
    :return: Список публикаций в соавто��стве.
    """
    author_id = info.context.get("author", {}).get("id")
    if not author_id:
        return []
    q = query_shouts()
    q = q.filter(Shout.authors.any(id=author_id))
    return get_shouts_with_stats(q, limit, offset=offset)


@query.field("load_shouts_discussed")
@login_required
async def load_shouts_discussed(_, info, limit=50, offset=0):
    """
    Загрузка публикаций, которые обсуждались пользователем.

    :param info: Информация о контексте GraphQL.
    :param limit: Максимальное количество публикаций.
    :param offset: Смещене для пагинации.
    :return: Список публикаций, обсужденных пользователем.
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
    q = query_shouts()
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
            q1 = query_shouts()
            q1 = q1.filter(Shout.authors.any(id=follower_id))

            # Публикации, на которые подписчик реагировал
            q2 = query_shouts()
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
