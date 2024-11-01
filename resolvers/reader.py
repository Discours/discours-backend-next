import json

from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import and_, asc, case, desc, func, nulls_last, select

from orm.author import Author
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from services.db import local_session
from services.schema import query
from services.search import search_text
from services.viewed import ViewedStorage
from utils.logger import root_logger as logger


def has_field(info, fieldname: str) -> bool:
    """
    Проверяет, запрошено ли поле :fieldname: в GraphQL запросе

    :param info: Информация о контексте GraphQL
    :param fieldname: Имя запрашиваемого поля
    :return: True, если поле запрошено, False в противном случае
    """
    field_node = info.field_nodes[0]
    for selection in field_node.selection_set.selections:
        if hasattr(selection, "name") and selection.name.value == fieldname:
            return True
    return False


def query_with_stat():
    """
    добавляет подзапрос статистики

    :return: Запрос с подзапросом статистики.
    """
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
    Оптимизированное получение публикаций с минимизацией количества запросов.
    """
    if author_id:
        q = q.filter(Shout.created_by == author_id)

    if limit:
        q = q.limit(limit)
    if offset:
        q = q.offset(offset)

    # Предварительно определяем флаги для запрашиваемых полей
    includes_authors = has_field(info, "authors")
    includes_topics = has_field(info, "topics")
    includes_stat = has_field(info, "stat")
    includes_media = has_field(info, "media")

    #  created_by и main_topic
    if has_field(info, "created_by"):
        q = q.outerjoin(Author, Shout.created_by == Author.id).add_columns(
            Author.id.label("main_author_id"),
            Author.name.label("main_author_name"),
            Author.slug.label("main_author_slug"),
            Author.pic.label("main_author_pic"),
            # Author.caption.label("main_author_caption"),
        )
    if has_field(info, "main_topic"):
        q = q.outerjoin(ShoutTopic, and_(ShoutTopic.shout == Shout.id, ShoutTopic.main.is_(True)))
        q = q.outerjoin(Topic, ShoutTopic.topic == Topic.id)
        q = q.add_columns(
            Topic.id.label("main_topic_id"),
            Topic.title.label("main_topic_title"),
            Topic.slug.label("main_topic_slug"),
            # func.literal(True).label("main_topic_is_main"),
        )

    with local_session() as session:
        shouts_result = session.execute(q).all()
        if not shouts_result:
            return []

        shout_ids = [shout.Shout.id for shout in shouts_result]
        authors_and_topics = []
        if includes_authors or includes_topics:
            query = (
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
            )
            authors_and_topics = session.execute(query).all()

        # Создаем словарь для хранения данных публикаций
        shouts_data = {}
        for row in shouts_result:
            shout_dict = row.Shout.dict()
            shout_dict["authors"] = []
            shout_dict["topics"] = set()

            # Добавляем данные main_author_, если они были запрошены
            if has_field(info, "created_by"):
                main_author = {
                    "id": row.main_author_id,
                    "name": row.main_author_name or "Аноним",
                    "slug": row.main_author_slug or "",
                    "pic": row.main_author_pic or "",
                    "caption": row.main_author_caption or "",
                }
                shout_dict["created_by"] = main_author

            # Добавляем данные main_topic, если они были запрошены
            if has_field(info, "main_topic"):
                main_topic = {
                    "id": row.main_topic_id or 0,
                    "title": row.main_topic_title or "",
                    "slug": row.main_topic_slug or "",
                    # "is_main": True,
                }
                shout_dict["main_topic"] = main_topic

            shouts_data[row.id] = shout_dict

        # Обрабатываем данные authors и topics из дополнительного запроса
        for row in authors_and_topics:
            shout_data = shouts_data.get(row.shout_id)
            if not shout_data:
                continue  # Пропускаем, если shout не найден

            if includes_authors:
                author = {
                    "id": row.author_id,
                    "name": row.author_name,
                    "slug": row.author_slug,
                    "pic": row.author_pic,
                    "caption": row.author_caption,
                }
                if author not in shout_data["authors"]:
                    shout_data["authors"].append(author)

            if includes_topics and row.topic_id:
                topic = {
                    "id": row.topic_id,
                    "title": row.topic_title,
                    "slug": row.topic_slug,
                    "is_main": row.topic_is_main,
                }
                shout_data["topics"].add(tuple(topic.items()))

        # Обрабатываем дополнительные поля и гарантируем наличие main_topic
        for shout in shouts_data.values():
            if includes_media:
                shout["media"] = json.dumps(shout.get("media", []))
            if includes_stat:
                shout_id = shout["id"]
                viewed_stat = ViewedStorage.get_shout(shout_id=shout_id) or 0
                shout["stat"] = {
                    "viewed": viewed_stat,
                    "commented": shout.get("comments_count", 0),
                    "rating": shout.get("rating", 0),
                    "last_reacted_at": shout.get("last_reacted_at"),
                }

            # Гарантируем наличие main_topic, если оно не запрашивалось
            if not has_field(info, "main_topic"):
                if "main_topic" not in shout or not shout["main_topic"]:
                    logger.error(f"Shout ID {shout['id']} не имеет основной темы.")
                    shout["main_topic"] = {
                        "id": 0,
                        "title": "Основная тема",
                        "slug": "",
                        # "is_main": True,
                    }

            # Сортировка topics, если они есть
            if shout["topics"]:
                shout["topics"] = sorted(
                    [dict(t) for t in shout["topics"]], key=lambda x: (not x.get("is_main", False), x["id"])
                )
            else:
                shout["topics"] = []

        return list(shouts_data.values())


def apply_filters(q, filters):
    """
    Применение общих фильтров к запросу.

    :param q: Исходный запрос.
    :param filters: Словарь фильтров.
    :return: Запрос с примененными фильтрами.
    """
    if isinstance(filters, dict):
        if "featured" in filters:
            featured_filter = filters.get("featured")
            if featured_filter:
                q = q.filter(Shout.featured_at.is_not(None))
            else:
                q = q.filter(Shout.featured_at.is_(None))
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
    :param info: Информация о контексте GraphQL
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


def apply_sorting(q, options):
    """
    Применение сортировки к запросу.

    :param q: Исходный запрос.
    :param options: Опции фильтрации и сортировки.
    :return: Запрос с примененной сортировкой.
    """
    # Определение поля для сортировки
    order_str = options.get("order_by")

    # Проверка, требуется ли сортировка по одному из статистических полей
    if order_str in ["rating", "comments_count", "last_reacted_at"]:
        # Сортировка по выбранному статистическому полю в указанном порядке
        q = q.order_by(desc(order_str))
        query_order_by = desc(order_str) if options.get("order_by_desc", True) else asc(order_str)
        # Применение сортировки с размещением NULL значений в конце
        q = q.order_by(nulls_last(query_order_by))
    else:
        q = q.order_by(Shout.published_at.desc())

    return q


@query.field("load_shouts_by")
async def load_shouts_by(_, info, options):
    """
    Загрузка публикаций с фильтрацией, сортировкой и пагинацией.

    :param _: Корневой объект запроса (не используется)
    :param info: Информация о контексте GraphQL
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций, удовлетворяющих критериям.
    """
    # Базовый запрос: если запрашиваются статистические данные, используем специальный запрос с статистикой
    q = (
        query_with_stat()
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )

    filters = options.get("filters")
    if isinstance(filters, dict):
        q = apply_filters(q, filters)

    q = apply_sorting(q, options)

    # Установка лимита и смещения для пагинации
    offset = options.get("offset", 0)
    limit = options.get("limit", 10)

    # Передача сформированного запроса в метод получения публикаций с учетом сортировки и пагинации
    return get_shouts_with_links(info, q, limit, offset)


@query.field("load_shouts_search")
async def load_shouts_search(_, info, text, options):
    """
    Поиск публикаций по тексту.

    :param _: Корневой объект запроса (не используется)
    :param info: Информация о контексте GraphQL
    :param text: Строка поиска.
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций, найденных по тексту.
    """
    limit = options.get("limit", 10)
    offset = options.get("offset", 0)
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
        q = apply_filters(q, options)
        q = apply_sorting(q, options)
        shouts = get_shouts_with_links(info, q, limit, offset)
        for shout in shouts:
            shout.score = scores[f"{shout.id}"]
        shouts.sort(key=lambda x: x.score, reverse=True)
        return shouts
    return []


@query.field("load_shouts_unrated")
async def load_shouts_unrated(_, info, options):
    """
    Загрузка публикаций с менее чем 3 реакциями типа LIKE/DISLIKE

    :param _: Корневой объект запроса (не используется)
    :param info: Информация о контексте GraphQL
    :param options: Опции фильтрации и сортировки.
    :return: Список публикаций.
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
        # .order_by(desc(Shout.published_at))
        .order_by(func.random())
    )
    limit = options.get("limit", 5)
    offset = options.get("offset", 0)
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

    subquery = select(Shout.id).outerjoin(aliased_reaction).where(Shout.deleted_at.is_(None))

    filters = options.get("filters")
    if isinstance(filters, dict):
        subquery = apply_filters(subquery, filters)

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
