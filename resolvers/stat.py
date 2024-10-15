import asyncio

from sqlalchemy import and_, distinct, func, join, select
from sqlalchemy.orm import aliased

from cache.cache import cache_author
from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.db import local_session
from utils.logger import root_logger as logger


def add_topic_stat_columns(q):
    """
    Добавляет статистические колонки к запросу тем.

    :param q: SQL-запрос для получения тем.
    :return: Запрос с добавленными колонками статистики.
    """
    # Создаем алиасы для предотвращения конфликтов имен
    aliased_shout = aliased(ShoutTopic)

    # Создаем новый объект запроса для тем
    new_q = select(Topic)

    # Применяем необходимые фильтры и добавляем колонки статистики
    new_q = (
        new_q.join(
            aliased_shout,
            aliased_shout.topic == Topic.id,
        )
        .join(
            Shout,
            and_(
                aliased_shout.shout == Shout.id,
                Shout.deleted_at.is_(None),
            ),
        )
        .add_columns(
            func.count(distinct(aliased_shout.shout)).label("shouts_stat")
        )  # Подсчет уникальных публикаций для темы
    )

    aliased_follower = aliased(TopicFollower)

    # Добавляем количество подписчиков темы
    new_q = new_q.outerjoin(aliased_follower, aliased_follower.topic == Topic.id).add_columns(
        func.count(distinct(aliased_follower.follower)).label("followers_stat")
    )

    # Группировка по идентификатору темы
    new_q = new_q.group_by(Topic.id)

    return new_q


def add_author_stat_columns(q):
    """
    Добавляет статистические колонки к запросу авторов.

    :param q: SQL-запрос для получения авторов.
    :return: Запрос с добавленными колонками статистики.
    """
    # Алиасирование таблиц для предотвращения конфликтов имен
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_shout = aliased(Shout)
    aliased_author_follower = aliased(AuthorFollower)

    # Применение фильтров и добавление колонок статистики
    q = (
        q.select_from(Author)
        .join(
            aliased_shout_author,
            aliased_shout_author.author == Author.id,
        )
        .join(
            aliased_shout,
            and_(
                aliased_shout.id == aliased_shout_author.shout,
                aliased_shout.deleted_at.is_(None),
            ),
        )
        .add_columns(
            func.count(distinct(aliased_shout.id)).label("shouts_stat")
        )  # Подсчет уникальных публикаций автора
    )

    # Добавляем количество подписчиков автора
    q = q.outerjoin(aliased_author_follower, aliased_author_follower.author == Author.id).add_columns(
        func.count(distinct(aliased_author_follower.follower)).label("followers_stat")
    )

    # Группировка по идентификатору автора
    q = q.group_by(Author.id)

    return q


def get_topic_shouts_stat(topic_id: int) -> int:
    """
    Получает количество публикаций для указанной темы.

    :param topic_id: Идентификатор темы.
    :return: Количество уникальных публикаций для темы.
    """
    q = (
        select(func.count(distinct(ShoutTopic.shout)))
        .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
        .filter(
            and_(
                ShoutTopic.topic == topic_id,
                Shout.published_at.is_not(None),
                Shout.deleted_at.is_(None),
            )
        )
    )
    # Выполнение запроса и получение результата
    with local_session() as session:
        result = session.execute(q).first()
    return result[0] if result else 0


def get_topic_authors_stat(topic_id: int) -> int:
    """
    Получает количество уникальных авторов для указанной темы.

    :param topic_id: Идентификатор темы.
    :return: Количество уникальных авторов, связанных с темой.
    """
    count_query = (
        select(func.count(distinct(ShoutAuthor.author)))
        .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
        .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
        .filter(
            and_(
                ShoutTopic.topic == topic_id,
                Shout.published_at.is_not(None),
                Shout.deleted_at.is_(None),
            )
        )
    )

    # Выполнение запроса и получение результата
    with local_session() as session:
        result = session.execute(count_query).first()
    return result[0] if result else 0


def get_topic_followers_stat(topic_id: int) -> int:
    """
    Получает количество подписчиков для указанной темы.

    :param topic_id: Идентификатор темы.
    :return: Количество уникальных подписчиков темы.
    """
    aliased_followers = aliased(TopicFollower)
    q = select(func.count(distinct(aliased_followers.follower))).filter(aliased_followers.topic == topic_id)
    with local_session() as session:
        result = session.execute(q).first()
    return result[0] if result else 0


def get_topic_comments_stat(topic_id: int) -> int:
    """
    Получает количество комментариев для всех публикаций в указанной теме.

    :param topic_id: Идентификатор темы.
    :return: Общее количество комментариев к публикациям темы.
    """
    # Подзапрос для получения количества комментариев для каждой публикации
    sub_comments = (
        select(
            Shout.id.label("shout_id"),
            func.coalesce(func.count(Reaction.id), 0).label("comments_count"),
        )
        .join(ShoutTopic, ShoutTopic.shout == Shout.id)
        .join(Topic, ShoutTopic.topic == Topic.id)
        .outerjoin(
            Reaction,
            and_(
                Reaction.shout == Shout.id,
                Reaction.kind == ReactionKind.COMMENT.value,
                Reaction.deleted_at.is_(None),
            ),
        )
        .group_by(Shout.id)
        .subquery()
    )
    # Запрос для суммирования количества комментариев по теме
    q = select(func.coalesce(func.sum(sub_comments.c.comments_count), 0)).filter(ShoutTopic.topic == topic_id)
    q = q.outerjoin(sub_comments, ShoutTopic.shout == sub_comments.c.shout_id)
    with local_session() as session:
        result = session.execute(q).first()
    return result[0] if result else 0


def get_author_shouts_stat(author_id: int) -> int:
    """
    Получает количество публикаций для указанного автора.

    :param author_id: Идентификатор автора.
    :return: Количество уникальных публикаций автора.
    """
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_shout = aliased(Shout)

    q = (
        select(func.count(distinct(aliased_shout.id)))
        .select_from(aliased_shout)
        .join(aliased_shout_author, aliased_shout.id == aliased_shout_author.shout)
        .filter(
            and_(
                aliased_shout_author.author == author_id,
                aliased_shout.published_at.is_not(None),
            )
        )
    )

    with local_session() as session:
        result = session.execute(q).first()

    return result[0] if result else 0


def get_author_authors_stat(author_id: int) -> int:
    """
    Получает количество авторов, на которых подписан указанный автор.

    :param author_id: Идентификатор автора.
    :return: Количество уникальных авторов, на которых подписан автор.
    """
    aliased_authors = aliased(AuthorFollower)
    q = select(func.count(distinct(aliased_authors.author))).filter(
        and_(
            aliased_authors.follower == author_id,
            aliased_authors.author != author_id,
        )
    )
    with local_session() as session:
        result = session.execute(q).first()
    return result[0] if result else 0


def get_author_followers_stat(author_id: int) -> int:
    """
    Получает количество подписчиков для указанного автора.

    :param author_id: Идентификатор автора.
    :return: Количество уникальных подписчиков автора.
    """
    aliased_followers = aliased(AuthorFollower)
    q = select(func.count(distinct(aliased_followers.follower))).filter(aliased_followers.author == author_id)
    with local_session() as session:
        result = session.execute(q).first()
    return result[0] if result else 0


def get_author_comments_stat(author_id):
    q = (
        select(func.coalesce(func.count(Reaction.id), 0).label("comments_count"))
        .select_from(Author)
        .outerjoin(
            Reaction,
            and_(
                Reaction.created_by == Author.id,
                Reaction.kind == ReactionKind.COMMENT.value,
                Reaction.deleted_at.is_(None),
            ),
        )
        .where(Author.id == author_id)
        .group_by(Author.id)
    )

    with local_session() as session:
        result = session.execute(q).first()
        return result.comments_count if result else 0


def get_with_stat(q):
    """
    Выполняет запрос с добавлением статистики.

    :param q: SQL-запрос для выполнения.
    :return: Список объектов с добавленной статистикой.
    """
    records = []
    try:
        with local_session() as session:
            # Определяем, является ли запрос запросом авторов
            author_prefixes = ("select author", "select * from author")
            is_author = f"{q}".lower().startswith(author_prefixes)

            # Добавляем колонки статистики в запрос
            q = add_author_stat_columns(q) if is_author else add_topic_stat_columns(q)

            # Выполняем запрос
            result = session.execute(q)
            for cols in result:
                entity = cols[0]
                stat = dict()
                stat["shouts"] = cols[1]  # Статистика по публикациям
                stat["followers"] = cols[2]  # Статистика по подписчикам
                if is_author:
                    stat["authors"] = get_author_authors_stat(entity.id)  # Статистика по подпискам на авторов
                    stat["comments"] = get_author_comments_stat(entity.id)  # Статистика по комментариям
                else:
                    stat["authors"] = get_topic_authors_stat(entity.id)  # Статистика по авторам темы
                entity.stat = stat
                records.append(entity)
    except Exception as exc:
        import traceback

        logger.debug(q)
        traceback.print_exc()
        logger.error(exc, exc_info=True)
    return records


def author_follows_authors(author_id: int):
    """
    Получает список авторов, на которых подписан указанный автор.

    :param author_id: Идентификатор автора.
    :return: Список авторов с добавленной статистикой.
    """
    af = aliased(AuthorFollower, name="af")
    author_follows_authors_query = (
        select(Author).select_from(join(Author, af, Author.id == af.author)).where(af.follower == author_id)
    )
    return get_with_stat(author_follows_authors_query)


def author_follows_topics(author_id: int):
    """
    Получает список тем, на которые подписан указанный автор.

    :param author_id: Идентификатор автора.
    :return: Список тем с добавленной статистикой.
    """
    author_follows_topics_query = (
        select(Topic)
        .select_from(join(Topic, TopicFollower, Topic.id == TopicFollower.topic))
        .where(TopicFollower.follower == author_id)
    )
    return get_with_stat(author_follows_topics_query)


def update_author_stat(author_id: int):
    """
    Обновляет статистику для указанного автора и сохраняет её в кэше.

    :param author_id: Идентификатор автора.
    """
    author_query = select(Author).where(Author.id == author_id)
    try:
        result = get_with_stat(author_query)
        if result:
            author_with_stat = result[0]
            if isinstance(author_with_stat, Author):
                author_dict = author_with_stat.dict()
                # Асинхронное кэширование данных автора
                asyncio.create_task(cache_author(author_dict))
    except Exception as exc:
        logger.error(exc, exc_info=True)
