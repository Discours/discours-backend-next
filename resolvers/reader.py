import json
import time

from sqlalchemy import text
from sqlalchemy.orm import aliased
from sqlalchemy.sql.expression import and_, asc, case, desc, func, nulls_last, select

from orm.author import Author
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from services.db import json_array_builder, json_builder, local_session
from services.schema import query
from services.search import search_text
from services.viewed import ViewedStorage
from utils.logger import root_logger as logger


def apply_options(q, options, reactions_created_by=0):
    """
    Применяет опции фильтрации и сортировки
    [опционально] выбирая те публикации, на которые есть реакции/комментарии от указанного автора

    :param q: Исходный запрос.
    :param options: Опции фильтрации и сортировки.
    :param reactions_created_by: Идентификатор автора.
    :return: Запрос с примененными опциями.
    """
    filters = options.get("filters")
    if isinstance(filters, dict):
        q = apply_filters(q, filters)
        if reactions_created_by:
            q = q.join(Reaction, Reaction.shout == Shout.id)
            q = q.filter(Reaction.created_by == reactions_created_by)
            if "commented" in filters:
                q = q.filter(Reaction.body.is_not(None))
    q = apply_sorting(q, options)
    limit = options.get("limit", 10)
    offset = options.get("offset", 0)
    return q, limit, offset


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


def query_with_stat(info):
    """
    Добавляет подзапрос статистики

    :param info: Информация о контексте GraphQL
    :return: Запрос с подзапросом статистики.
    """
    # Основной запрос
    q = (
        select(Shout)
        .distinct(Shout.id)
        .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        .join(Author, Author.id == Shout.created_by)
    )

    # Создаем алиасы для всех таблиц
    main_author = aliased(Author)
    main_topic_join = aliased(ShoutTopic)
    main_topic = aliased(Topic)
    
    # main_author
    q = q.join(main_author, main_author.id == Shout.created_by)
    q = q.add_columns(
        json_builder(
            "id", main_author.id, 
            "name", main_author.name, 
            "slug", main_author.slug, 
            "pic", main_author.pic
        ).label("main_author")
    )

    if has_field(info, "main_topic"):
        q = q.join(main_topic_join, and_(main_topic_join.shout == Shout.id, main_topic_join.main.is_(True))).join(
            main_topic, main_topic.id == main_topic_join.topic
        )
        q = q.add_columns(
            json_builder(
                "id", main_topic.id, "title", main_topic.title, "slug", main_topic.slug, "is_main", main_topic_join.main
            ).label("main_topic")
        )
        q = q.group_by(main_topic.id, main_topic.title, main_topic.slug, main_topic_join.main)

    if has_field(info, "topics"):
        topics_subquery = (
            select(
                json_array_builder(
                    json_builder("id", Topic.id, "title", Topic.title, "slug", Topic.slug, "is_main", ShoutTopic.main)
                ).label("topics")
            )
            .outerjoin(Topic, ShoutTopic.topic == Topic.id)
            .where(ShoutTopic.shout == Shout.id)
            .subquery()
        )
        q = q.outerjoin(topics_subquery, topics_subquery.c.shout == Shout.id)
        q = q.add_columns(topics_subquery.c.topics)
        q = q.group_by(topics_subquery.c.topics)

    if has_field(info, "authors"):
        authors_subquery = (
            select(
                json_array_builder(
                    json_builder(
                        "id", Author.id,
                        "name", Author.name,
                        "slug", Author.slug,
                        "pic", Author.pic,
                        "caption", ShoutAuthor.caption,
                    )
                ).label("authors")
            )
            .outerjoin(Author, ShoutAuthor.author == Author.id)
            .where(ShoutAuthor.shout == Shout.id)
            .subquery()
        )
        q = q.outerjoin(authors_subquery, authors_subquery.c.shout == Shout.id)
        q = q.add_columns(authors_subquery.c.authors)
        q = q.group_by(authors_subquery.c.authors)

    if has_field(info, "stat"):
        stats_subquery = (
            select(
                Reaction.shout,
                func.count(func.distinct(Reaction.id))
                .filter(Reaction.kind == ReactionKind.COMMENT.value)
                .label("comments_count"),
                func.coalesce(
                    func.sum(
                        case(
                            (Reaction.reply_to.is_not(None), 0),
                            (Reaction.kind == ReactionKind.LIKE.value, 1),
                            (Reaction.kind == ReactionKind.DISLIKE.value, -1),
                            else_=0,
                        )
                    ),
                    0,
                ).label("rating"),
                func.coalesce(func.max(case((Reaction.reply_to.is_(None), Reaction.created_at), else_=None)), 0).label(
                    "last_reacted_at"
                ),
            )
            .where(Reaction.deleted_at.is_(None))
            .group_by(Reaction.shout)
            .subquery()
        )

        q = q.outerjoin(stats_subquery, stats_subquery.c.shout == Shout.id)
        # aggregate in one column
        q = q.add_columns(
            json_builder(
                "comments_count", stats_subquery.c.comments_count,
                "rating", stats_subquery.c.rating,
                "last_reacted_at", stats_subquery.c.last_reacted_at,
            ).label("stat")
        )
        q = q.group_by(stats_subquery.c.comments_count, stats_subquery.c.rating, stats_subquery.c.last_reacted_at)

    return q


def get_shouts_with_links(info, q, limit=20, offset=0):
    """
    получение публикаций с применением пагинации
    """
    shouts = []
    try:
        logger.info(f"Starting get_shouts_with_links with limit={limit}, offset={offset}")
        q = q.limit(limit).offset(offset)

        with local_session() as session:
            logger.info("Executing query...")
            t1 = time.time()
            shouts_result = session.execute(q).all()
            logger.info(f"Query executed, got {len(shouts_result)} results in {time.time() - t1:.3f} seconds")

            if not shouts_result:
                logger.warning("No results found")
                return []

            shouts = []

            for idx, row in enumerate(shouts_result):
                try:
                    # logger.debug(row)
                    shout = None
                    if hasattr(row, "Shout"):
                        shout = row.Shout
                    else:
                        logger.warning(f"Row {idx} has no Shout attribute: {row}")
                        continue

                    if shout:
                        shout_id = int(f"{shout.id}")
                        # logger.info(f"Processing shout ID: {shout_id}")
                        shout_dict = shout.dict()
                        if has_field(info, "created_by") and shout_dict.get("created_by"):
                            main_author_id = shout_dict.get("created_by")
                            a = session.query(Author).filter(Author.id == main_author_id).first()
                            shout_dict["created_by"] = {
                                "id": main_author_id,
                                "name": a.id,
                                "slug": a.slug,
                                "pic": a.pic,
                            }
                        if hasattr(row, "stat"):
                            stat = {}
                            # logger.debug(row.stat)
                            if isinstance(row.stat, str):
                                stat = json.loads(row.stat)
                            elif isinstance(row.stat, dict):
                                stat = row.stat
                            viewed = ViewedStorage.get_shout(shout_id=shout_id) or 0
                            shout_dict["stat"] = {
                                **stat, 
                                "viewed": viewed,
                                "commented": stat.get("comments_count", 0)
                            }

                        if has_field(info, "main_topic") and hasattr(row, "main_topic"):
                            shout_dict["main_topic"] = json.loads(row.main_topic) if isinstance(row.stat, str) else row.main_topic
                        if has_field(info, "authors") and hasattr(row, "authors"):
                            shout_dict["authors"] = json.loads(row.authors) if isinstance(row.authors, str) else row.authors
                        if has_field(info, "topics") and hasattr(row, "topics"):
                            shout_dict["topics"] = json.loads(row.topics) if isinstance(row.topics, str) else row.topics

                    shouts.append(shout_dict)

                except Exception as row_error:
                    logger.error(f"Error processing row {idx}: {row_error}", exc_info=True)
                    continue
    except Exception as e:
        logger.error(f"Fatal error in get_shouts_with_links: {e}", exc_info=True)
        raise
    finally:
        return shouts


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
        q = query_with_stat(info)

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
    Применение сортировки к запросу с учетом DISTINCT ON.
    """
    # Сначала создаем подзапрос с DISTINCT ON
    subq = q.order_by(Shout.id).subquery()
    
    # Создаем новый запрос к подзапросу
    q = select(subq)

    # Применяем нужную сортировку
    order_str = options.get("order_by")
    if order_str in ["rating", "comments_count", "last_reacted_at"]:
        query_order_by = desc(text(order_str)) if options.get("order_by_desc", True) else asc(text(order_str))
        q = q.order_by(nulls_last(query_order_by))
    else:
        q = q.order_by(subq.c.published_at.desc())

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
    q = query_with_stat(info)
    q, limit, offset = apply_options(q, options)

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
            query_with_stat(info)
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
        query_with_stat(info)
        if has_field(info, "stat")
        else select(Shout).filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
    )
    q = q.filter(Shout.id.in_(subquery))
    q = q.order_by(func.random())
    limit = options.get("limit", 10)
    return get_shouts_with_links(info, q, limit)
