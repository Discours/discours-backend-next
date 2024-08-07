import time

from sqlalchemy import and_, asc, case, desc, func, select
from sqlalchemy.orm import aliased

from orm.author import Author
from orm.rating import PROPOSAL_REACTIONS, RATING_REACTIONS, is_negative, is_positive
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout
from resolvers.editor import handle_proposing
from resolvers.follower import follow
from resolvers.stat import update_author_stat
from services.auth import add_user_role, login_required
from services.db import local_session
from utils.logger import root_logger as logger
from services.notify import notify_reaction
from services.schema import mutation, query


def add_reaction_stat_columns(q, aliased_reaction):
    """
    Добавляет статистические колонки к запросу реакций.

    :param q: SQL-запрос для реакций.
    :param aliased_reaction: Алиас для таблицы реакций.
    :return: Запрос с добавленными колонками статистики.
    """
    # Присоединение реакций и добавление статистических колонок
    q = q.outerjoin(aliased_reaction, aliased_reaction.deleted_at.is_(None)).add_columns(
        # Подсчет комментариев
        func.count(case((aliased_reaction.body.is_not(None), 1), else_=0)).label("comments_stat"),
        # Вычисление рейтинга как разница между лайками и дизлайками
        func.sum(
            case(
                (aliased_reaction.kind == ReactionKind.LIKE.value, 1),
                (aliased_reaction.kind == ReactionKind.DISLIKE.value, -1),
                else_=0,
            )
        ).label("rating_stat"),
    )

    return q


def is_featured_author(session, author_id) -> bool:
    """
    Проверяет, есть ли у автора хотя бы одна опубликованная статья.

    :param session: Сессия базы данных.
    :param author_id: Идентификатор автора.
    :return: True, если у автора есть хотя бы одна опубликованная статья, иначе False.
    """
    return (
        session.query(Shout)
        .where(Shout.authors.any(id=author_id))
        .filter(and_(Shout.featured_at.is_not(None), Shout.deleted_at.is_(None)))
        .count()
        > 0
    )


def check_to_feature(session, approver_id, reaction) -> bool:
    """
    Устанавливает публикацию в открытый доступ, если количество голосов превышает 4.

    :param session: Сессия базы данных.
    :param approver_id: Идентификатор утверждающего автора.
    :param reaction: Объект реакции.
    :return: True, если нужно установить публикацию в открытый доступ, иначе False.
    """
    if not reaction.reply_to and is_positive(reaction.kind):
        if is_featured_author(session, approver_id):
            approvers = [approver_id]
            # Подсчет количества голосующих утверждающих
            reacted_readers = session.query(Reaction).where(Reaction.shout == reaction.shout).all()
            for reacted_reader in reacted_readers:
                if is_featured_author(session, reacted_reader.id):
                    approvers.append(reacted_reader.id)
            if len(approvers) > 4:
                return True
    return False


def check_to_unfeature(session, rejecter_id, reaction) -> bool:
    """
    Убирает публикацию из открытого доступа, если 20% реакций негативные.

    :param session: Сессия базы данных.
    :param rejecter_id: Идентификатор отклоняющего автора.
    :param reaction: Объект реакции.
    :return: True, если нужно убрать публикацию из открытого доступа, иначе False.
    """
    if not reaction.reply_to and is_negative(reaction.kind):
        if is_featured_author(session, rejecter_id):
            reactions = (
                session.query(Reaction)
                .where(
                    and_(
                        Reaction.shout == reaction.shout,
                        Reaction.kind.in_(RATING_REACTIONS),
                    )
                )
                .all()
            )
            rejects = 0
            for r in reactions:
                approver = session.query(Author).filter(Author.id == r.created_by).first()
                if is_featured_author(session, approver):
                    if is_negative(r.kind):
                        rejects += 1
            if len(reactions) / rejects < 5:
                return True
    return False


async def set_featured(session, shout_id):
    """
    Устанавливает публикацию в открытый доступ и обновляет роль автора.

    :param session: Сессия базы данных.
    :param shout_id: Идентификатор публикации.
    """
    s = session.query(Shout).where(Shout.id == shout_id).first()
    s.featured_at = int(time.time())
    Shout.update(s, {"featured_at": int(time.time())})
    author = session.query(Author).filter(Author.id == s.created_by).first()
    if author:
        await add_user_role(str(author.user))
    session.add(s)
    session.commit()


def set_unfeatured(session, shout_id):
    """
    Убирает публикацию из открытого доступа.

    :param session: Сессия базы данных.
    :param shout_id: Идентификатор публикации.
    """
    s = session.query(Shout).where(Shout.id == shout_id).first()
    Shout.update(s, {"featured_at": None})
    session.add(s)
    session.commit()


async def _create_reaction(session, info, shout, author_id: int, reaction) -> dict:
    """
    Создает новую реакцию и выполняет связанные с этим действия, такие как обновление счетчиков и уведомление.

    :param session: Сессия базы данных.
    :param info: Информация о контексте GraphQL.
    :param shout: Объект публикации.
    :param author_id: Идентификатор автора.
    :param reaction: Словарь с данными реакции.
    :return: Словарь с данными о созданной реакции.
    """
    r = Reaction(**reaction)
    session.add(r)
    session.commit()
    rdict = r.dict()

    # Пересчет счетчика комментариев
    if str(r.kind) == ReactionKind.COMMENT.value:
        update_author_stat(author_id)

    # Совместное редактирование
    if rdict.get("reply_to") and r.kind in PROPOSAL_REACTIONS and author_id in shout.authors:
        handle_proposing(session, r, shout)

    # Рейтинг и саморегуляция
    if r.kind in RATING_REACTIONS:
        # Механизм саморегуляции
        if check_to_unfeature(session, author_id, r):
            set_unfeatured(session, shout.id)
        elif check_to_feature(session, author_id, r):
            await set_featured(session, shout.id)

        # Подписка, если понравилось
        if r.kind == ReactionKind.LIKE.value:
            try:
                # Автоподписка при реакции
                follow(None, info, "shout", shout.slug)
            except Exception:
                pass

    # Обновление счетчика комментариев в кэше
    if str(r.kind) == ReactionKind.COMMENT.value:
        update_author_stat(author_id)

    rdict["shout"] = shout.dict()
    rdict["stat"] = {"commented": 0, "reacted": 0, "rating": 0}

    # Уведомление о создании
    await notify_reaction(rdict, "create")

    return rdict


def prepare_new_rating(reaction: dict, shout_id: int, session, author_id: int):
    """
    Проверяет возможность выставления новой оценки для публикации.

    :param reaction: Словарь с данными реакции.
    :param shout_id: Идентификатор публикации.
    :param session: Сессия базы данных.
    :param author_id: Идентификатор автора.
    :return: Словарь с ошибкой или None.
    """
    kind = reaction.get("kind")
    opposite_kind = ReactionKind.DISLIKE.value if is_positive(kind) else ReactionKind.LIKE.value

    # Формирование запроса для проверки существующих оценок
    q = select(Reaction).filter(
        and_(
            Reaction.shout == shout_id,
            Reaction.created_by == author_id,
            Reaction.kind.in_(RATING_REACTIONS),
            Reaction.deleted_at.is_not(None),
        )
    )
    reply_to = reaction.get("reply_to")
    if reply_to and isinstance(reply_to, int):
        q = q.filter(Reaction.reply_to == reply_to)
    rating_reactions = session.execute(q).all()

    # Проверка условий для выставления новой оценки
    if rating_reactions:
        same_rating = filter(
            lambda r: r.created_by == author_id and r.kind == kind,
            rating_reactions,
        )
        opposite_rating = filter(
            lambda r: r.created_by == author_id and r.kind == opposite_kind,
            rating_reactions,
        )
        if same_rating:
            return {"error": "You can't rate the same thing twice"}
        elif opposite_rating:
            return {"error": "Remove opposite vote first"}
        elif filter(lambda r: r.created_by == author_id, rating_reactions):
            return {"error": "You can't rate your own thing"}
    return


@mutation.field("create_reaction")
@login_required
async def create_reaction(_, info, reaction):
    """
    Создает новую реакцию через GraphQL запрос.

    :param info: Информация о контексте GraphQL.
    :param reaction: Словарь с данными реакции.
    :return: Словарь с информацией о созданной реакции или ошибкой.
    """
    # logger.debug(f"{info.context} for {reaction}")
    info.context.get("user_id")
    author_dict = info.context.get("author", {})
    if not isinstance(author_dict, dict):
        return {"error": "Unauthorized"}
    author_id = author_dict.get("id")
    shout_id = reaction.get("shout")
    if not shout_id:
        return {"error": "Shout ID is required to create a reaction."}

    try:
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            if shout and author_id:
                reaction["created_by"] = int(author_id)
                kind = reaction.get("kind")

                if not kind and isinstance(reaction.get("body"), str):
                    kind = ReactionKind.COMMENT.value

                if not kind:
                    return {"error": "cannot create reaction without a kind"}

                if kind in RATING_REACTIONS:
                    error_result = prepare_new_rating(reaction, shout_id, session, author_id)
                    if error_result:
                        return error_result

                rdict = await _create_reaction(session, info, shout, author_id, reaction)

                # TODO: call recount ratings periodically
                rdict["created_by"] = author_dict
                return {"reaction": rdict}
    except Exception as e:
        import traceback

        traceback.print_exc()
        logger.error(f"{type(e).__name__}: {e}")

    return {"error": "Cannot create reaction."}


@mutation.field("update_reaction")
@login_required
async def update_reaction(_, info, reaction):
    """
    Обновляет существующую реакцию через GraphQL запрос.

    :param info: Информация о контексте GraphQL.
    :param reaction: Словарь с данными реакции.
    :return: Словарь с информацией об обновленной реакции или ошибкой.
    """
    logger.debug(f"{info.context} for {reaction}")
    user_id = info.context.get("user_id")
    roles = info.context.get("roles")
    rid = reaction.get("id")
    if rid and isinstance(rid, int) and user_id and roles:
        del reaction["id"]
        with local_session() as session:
            reaction_query = select(Reaction).filter(Reaction.id == rid)
            aliased_reaction = aliased(Reaction)
            reaction_query = add_reaction_stat_columns(reaction_query, aliased_reaction)
            reaction_query = reaction_query.group_by(Reaction.id)

            try:
                result = session.execute(reaction_query).unique().first()
                if result:
                    [r, commented_stat, rating_stat] = result
                    if not r:
                        return {"error": "invalid reaction id"}

                    author = session.query(Author).filter(Author.user == user_id).first()
                    if author:
                        if r.created_by != author.id and "editor" not in roles:
                            return {"error": "access denied"}

                        body = reaction.get("body")
                        if body:
                            r.body = body
                        r.updated_at = int(time.time())

                        if r.kind != reaction["kind"]:
                            # Определение изменения мнения может быть реализовано здесь
                            pass

                        Reaction.update(r, reaction)
                        session.add(r)
                        session.commit()

                        r.stat = {
                            "commented": commented_stat,
                            "rating": rating_stat,
                        }

                        await notify_reaction(r.dict(), "update")

                        return {"reaction": r}
                    else:
                        return {"error": "not authorized"}
            except Exception:
                import traceback

                traceback.print_exc()
    return {"error": "cannot create reaction"}


@mutation.field("delete_reaction")
@login_required
async def delete_reaction(_, info, reaction_id: int):
    """
    Удаляет существующую реакцию через GraphQL запрос.

    :param info: Информация о контексте GraphQL.
    :param reaction_id: Идентификатор удаляемой реакции.
    :return: Словарь с информацией об удаленной реакции или ошибкой.
    """
    logger.debug(f"{info.context} for {reaction_id}")
    user_id = info.context.get("user_id")
    author_id = info.context.get("author", {}).get("id")
    roles = info.context.get("roles", [])
    if user_id:
        with local_session() as session:
            try:
                author = session.query(Author).filter(Author.user == user_id).one()
                r = session.query(Reaction).filter(Reaction.id == reaction_id).one()
                if r.created_by != author_id and "editor" not in roles:
                    return {"error": "access denied"}

                logger.debug(f"{user_id} user removing his #{reaction_id} reaction")
                reaction_dict = r.dict()
                session.delete(r)
                session.commit()

                # Обновление счетчика комментариев в кэше
                if str(r.kind) == ReactionKind.COMMENT.value:
                    update_author_stat(author.id)
                await notify_reaction(reaction_dict, "delete")

                return {"error": None, "reaction": reaction_dict}
            except Exception as exc:
                return {"error": f"cannot delete reaction: {exc}"}
    return {"error": "cannot delete reaction"}


def apply_reaction_filters(by, q):
    """
    Применяет фильтры к запросу реакций.

    :param by: Словарь с параметрами фильтрации.
    :param q: SQL-запрос.
    :return: Запрос с примененными фильтрами.
    """
    shout_slug = by.get("shout", None)
    if shout_slug:
        q = q.filter(Shout.slug == shout_slug)

    elif by.get("shouts"):
        q = q.filter(Shout.slug.in_(by.get("shouts", [])))

    created_by = by.get("created_by", None)
    if created_by:
        q = q.filter(Author.id == created_by)

    author_slug = by.get("author", None)
    if author_slug:
        q = q.filter(Author.slug == author_slug)

    topic = by.get("topic", None)
    if isinstance(topic, int):
        q = q.filter(Shout.topics.any(id=topic))

    if by.get("comment", False):
        q = q.filter(Reaction.kind == ReactionKind.COMMENT.value)

    if by.get("rating", False):
        q = q.filter(Reaction.kind.in_(RATING_REACTIONS))

    by_search = by.get("search", "")
    if len(by_search) > 2:
        q = q.filter(Reaction.body.ilike(f"%{by_search}%"))

    after = by.get("after", None)
    if isinstance(after, int):
        q = q.filter(Reaction.created_at > after)

    return q


@query.field("load_reactions_by")
async def load_reactions_by(_, info, by, limit=50, offset=0):
    """
    Загружает реакции по указанным параметрам.

    :param info: Информация о контексте GraphQL.
    :param by: {
        :shout - фильтрация по slug публикации
        :shouts - фильтрация по списку slug публикаций
        :created_by - фильтрация по идентификатору автора
        :author - фильтрация по slug автора
        :topic - фильтрация по теме
        :search - поиск по тексту реакций
        :comment - фильтрация комментариев
        :rating - фильтрация реакций с рейтингом
        :after - фильтрация по времени создания
        :sort - поле для сортировки (по убыванию по умолчанию)
    }
    :param limit: Количество реакций для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список реакций.
    """
    q = (
        select(Reaction, Author, Shout)
        .select_from(Reaction)
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    # Подсчет статистики
    aliased_reaction = aliased(Reaction)
    q = add_reaction_stat_columns(q, aliased_reaction)

    # Применение фильтров
    q = apply_reaction_filters(by, q)
    q = q.where(Reaction.deleted_at.is_(None))

    # Группировка
    q = q.group_by(Reaction.id, Author.id, Shout.id, aliased_reaction.id)

    # Сортировка
    order_stat = by.get("sort", "").lower()  # 'like' | 'dislike' | 'newest' | 'oldest'
    order_by_stmt = desc(Reaction.created_at)
    if order_stat == "oldest":
        order_by_stmt = asc(Reaction.created_at)
    elif order_stat.endswith("like"):
        order_by_stmt = desc("rating_stat")
    q = q.order_by(order_by_stmt)

    # Пагинация
    q = q.limit(limit).offset(offset)

    reactions = set()
    with local_session() as session:
        result_rows = session.execute(q)
        for [
            reaction,
            author,
            shout,
            commented_stat,
            rating_stat,
            last_reacted_at,
        ] in result_rows:
            reaction.created_by = author
            reaction.shout = shout
            reaction.stat = {"rating": rating_stat, "commented": commented_stat}
            reactions.add(reaction)

    return reactions


@query.field("load_shout_ratings")
async def load_shout_ratings(_, info, shout: int, limit=100, offset=0):
    """
    Получает оценки для указанной публикации с пагинацией.

    :param info: Информация о контексте GraphQL.
    :param shout: Идентификатор публикации.
    :param limit: Количество реакций для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список реакций.
    """
    q = (
        select(Reaction, Author, Shout)
        .select_from(Reaction)
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    # Фильтрация, группировка, сортировка, лимит, офсет
    q = q.filter(and_(Reaction.deleted_at.is_(None), Reaction.shout == shout, Reaction.kind.in_(RATING_REACTIONS)))
    q = q.group_by(Reaction.id)
    q = q.order_by(desc(Reaction.created_at))
    q = q.limit(limit).offset(offset)

    reactions = set()
    with local_session() as session:
        result_rows = session.execute(q)
        for [
            reaction,
            author,
            shout,
        ] in result_rows:
            reaction.created_by = author
            reaction.shout = shout
            reactions.add(reaction)

    return reactions


@query.field("load_shout_comments")
async def load_shout_comments(_, info, shout: int, limit=50, offset=0):
    """
    Получает комментарии для указанной публикации с пагинацией и статистикой.

    :param info: Информация о контексте GraphQL.
    :param shout: Идентификатор публикации.
    :param limit: Количество комментариев для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список реакций.
    """
    aliased_reaction = aliased(Reaction)
    q = (
        select(
            Reaction,
            Author,
            Shout,
            func.count(aliased_reaction.id).label("reacted_stat"),
            func.count(aliased_reaction.body).label("commented_stat"),
            func.sum(case((aliased_reaction.kind == str(ReactionKind.LIKE.value), 1), else_=0)).label("likes_stat"),
            func.sum(case((aliased_reaction.kind == str(ReactionKind.DISLIKE.value), 1), else_=0)).label(
                "dislikes_stat"
            ),
        )
        .select_from(Reaction)
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    # Фильтрация, группировка, сортировка, лимит, офсет
    q = q.filter(and_(Reaction.deleted_at.is_(None), Reaction.shout == shout, Reaction.body.is_not(None)))
    q = q.group_by(Reaction.id, Author.id, Shout.id)
    q = q.order_by(desc(Reaction.created_at))
    q = q.limit(limit).offset(offset)

    reactions = set()
    with local_session() as session:
        result_rows = session.execute(q)
        for row in result_rows:
            reaction, author, shout, reacted_stat, commented_stat, likes_stat, dislikes_stat = row
            reaction.created_by = author
            reaction.shout = shout
            reaction.stat = {
                "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                "reacted": reacted_stat,
                "commented": commented_stat,
            }
            reactions.add(reaction)

    return list(reactions)


@query.field("load_comment_ratings")
async def load_comment_ratings(_, info, comment: int, limit=50, offset=0):
    """
    Получает оценки для указанного комментария с пагинацией и статистикой.

    :param info: Информация о контексте GraphQL.
    :param comment: Идентификатор комментария.
    :param limit: Количество оценок для загрузки.
    :param offset: Смещение для пагинации.
    :return: Список реакций.
    """
    aliased_reaction = aliased(Reaction)
    q = (
        select(
            Reaction,
            Author,
            Shout,
            func.count(aliased_reaction.id).label("reacted_stat"),
            func.count(aliased_reaction.body).label("commented_stat"),
            func.sum(case((aliased_reaction.kind == str(ReactionKind.LIKE.value), 1), else_=0)).label("likes_stat"),
            func.sum(case((aliased_reaction.kind == str(ReactionKind.DISLIKE.value), 1), else_=0)).label(
                "dislikes_stat"
            ),
        )
        .select_from(Reaction)
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    # Фильтрация, группировка, сортировка, лимит, офсет
    q = q.filter(and_(Reaction.deleted_at.is_(None), Reaction.reply_to == comment, Reaction.body.is_not(None)))
    q = q.group_by(Reaction.id, Author.id, Shout.id)
    q = q.order_by(desc(Reaction.created_at))
    q = q.limit(limit).offset(offset)

    reactions = set()
    with local_session() as session:
        result_rows = session.execute(q)
        for row in result_rows:
            reaction, author, shout, reacted_stat, commented_stat, likes_stat, dislikes_stat = row
            reaction.created_by = author
            reaction.shout = shout
            reaction.stat = {
                "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                "reacted": reacted_stat,
                "commented": commented_stat,
            }
            reactions.add(reaction)

    return list(reactions)
