from sqlalchemy import and_, case, func, select, true
from sqlalchemy.orm import aliased

from orm.author import Author, AuthorRating
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from utils.logger import root_logger as logger


@query.field("get_my_rates_comments")
@login_required
async def get_my_rates_comments(_, info, comments: list[int]) -> list[dict]:
    """
    Получение реакций пользователя на комментарии

    Args:
        info: Контекст запроса
        comments: Список ID комментариев

    Returns:
        list[dict]: Список словарей с реакциями пользователя на комментарии
        Каждый словарь содержит:
            - comment_id: ID комментария
            - my_rate: Тип реакции (LIKE/DISLIKE)
    """
    author_dict = info.context.get("author") if info.context else None
    author_id = author_dict.get("id") if author_dict else None
    if not author_id:
        return []  # Возвращаем пустой список вместо словаря с ошибкой

    # Подзапрос для реакций текущего пользователя
    rated_query = (
        select(Reaction.id.label("comment_id"), Reaction.kind.label("my_rate"))
        .where(
            and_(
                Reaction.reply_to.in_(comments),
                Reaction.created_by == author_id,
                Reaction.deleted_at.is_(None),
                Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
            )
        )
        .order_by(Reaction.shout, Reaction.created_at.desc())
        .distinct(Reaction.shout)
    )
    with local_session() as session:
        comments_result = session.execute(rated_query).all()
        return [{"comment_id": row.comment_id, "my_rate": row.my_rate} for row in comments_result]


@query.field("get_my_rates_shouts")
@login_required
async def get_my_rates_shouts(_, info, shouts):
    """
    Получение реакций пользователя на публикации
    """
    author_dict = info.context.get("author") if info.context else None
    author_id = author_dict.get("id") if author_dict else None

    if not author_id:
        return []

    with local_session() as session:
        try:
            stmt = (
                select(Reaction)
                .where(
                    and_(
                        Reaction.shout.in_(shouts),
                        Reaction.reply_to.is_(None),
                        Reaction.created_by == author_id,
                        Reaction.deleted_at.is_(None),
                        Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
                    )
                )
                .order_by(Reaction.shout, Reaction.created_at.desc())
                .distinct(Reaction.shout)
            )

            result = session.execute(stmt).all()

            return [
                {
                    "shout_id": row[0].shout,  # Получаем shout_id из объекта Reaction
                    "my_rate": row[0].kind,  # Получаем kind (my_rate) из объекта Reaction
                }
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error in get_my_rates_shouts: {e}")
            return []


@mutation.field("rate_author")
@login_required
async def rate_author(_, info, rated_slug, value):
    info.context["user_id"]
    rater_id = info.context.get("author", {}).get("id")
    with local_session() as session:
        rater_id = int(rater_id)
        rated_author = session.query(Author).filter(Author.slug == rated_slug).first()
        if rater_id and rated_author:
            rating: AuthorRating = (
                session.query(AuthorRating)
                .filter(
                    and_(
                        AuthorRating.rater == rater_id,
                        AuthorRating.author == rated_author.id,
                    )
                )
                .first()
            )
            if rating:
                rating.plus = value > 0
                session.add(rating)
                session.commit()
                return {}
            else:
                try:
                    rating = AuthorRating(rater=rater_id, author=rated_author.id, plus=value > 0)
                    session.add(rating)
                    session.commit()
                except Exception as err:
                    return {"error": err}
    return {}


def count_author_comments_rating(session, author_id) -> int:
    replied_alias = aliased(Reaction)
    replies_likes = (
        session.query(replied_alias)
        .join(Reaction, replied_alias.id == Reaction.reply_to)
        .where(
            and_(
                replied_alias.created_by == author_id,
                replied_alias.kind == ReactionKind.COMMENT.value,
            )
        )
        .filter(replied_alias.kind == ReactionKind.LIKE.value)
        .count()
    ) or 0
    replies_dislikes = (
        session.query(replied_alias)
        .join(Reaction, replied_alias.id == Reaction.reply_to)
        .where(
            and_(
                replied_alias.created_by == author_id,
                replied_alias.kind == ReactionKind.COMMENT.value,
            )
        )
        .filter(replied_alias.kind == ReactionKind.DISLIKE.value)
        .count()
    ) or 0

    return replies_likes - replies_dislikes


def count_author_shouts_rating(session, author_id) -> int:
    shouts_likes = (
        session.query(Reaction, Shout)
        .join(Shout, Shout.id == Reaction.shout)
        .filter(
            and_(
                Shout.authors.any(id=author_id),
                Reaction.kind == ReactionKind.LIKE.value,
            )
        )
        .count()
        or 0
    )
    shouts_dislikes = (
        session.query(Reaction, Shout)
        .join(Shout, Shout.id == Reaction.shout)
        .filter(
            and_(
                Shout.authors.any(id=author_id),
                Reaction.kind == ReactionKind.DISLIKE.value,
            )
        )
        .count()
        or 0
    )
    return shouts_likes - shouts_dislikes


def get_author_rating_old(session, author: Author):
    likes_count = (
        session.query(AuthorRating).filter(and_(AuthorRating.author == author.id, AuthorRating.plus.is_(True))).count()
    )
    dislikes_count = (
        session.query(AuthorRating)
        .filter(and_(AuthorRating.author == author.id, AuthorRating.plus.is_not(True)))
        .count()
    )
    return likes_count - dislikes_count


def get_author_rating_shouts(session, author: Author) -> int:
    q = (
        select(
            func.coalesce(
                func.sum(
                    case(
                        (Reaction.kind == ReactionKind.LIKE.value, 1),
                        (Reaction.kind == ReactionKind.DISLIKE.value, -1),
                        else_=0,
                    )
                ),
                0,
            ).label("shouts_rating")
        )
        .select_from(Reaction)
        .outerjoin(Shout, Shout.authors.any(id=author.id))
        .outerjoin(
            Reaction,
            and_(
                Reaction.reply_to.is_(None),
                Reaction.shout == Shout.id,
                Reaction.deleted_at.is_(None),
            ),
        )
    )
    result = session.execute(q).scalar()
    return result


def get_author_rating_comments(session, author: Author) -> int:
    replied_comment = aliased(Reaction)
    q = (
        select(
            func.coalesce(
                func.sum(
                    case(
                        (Reaction.kind == ReactionKind.LIKE.value, 1),
                        (Reaction.kind == ReactionKind.DISLIKE.value, -1),
                        else_=0,
                    )
                ),
                0,
            ).label("shouts_rating")
        )
        .select_from(Reaction)
        .outerjoin(
            Reaction,
            and_(
                replied_comment.kind == ReactionKind.COMMENT.value,
                replied_comment.created_by == author.id,
                Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
                Reaction.reply_to == replied_comment.id,
                Reaction.deleted_at.is_(None),
            ),
        )
    )
    result = session.execute(q).scalar()
    return result


def add_author_rating_columns(q, group_list):
    # NOTE: method is not used

    # old karma
    q = q.outerjoin(AuthorRating, AuthorRating.author == Author.id)
    q = q.add_columns(func.sum(case((AuthorRating.plus == true(), 1), else_=-1)).label("rating"))

    # by shouts rating
    shout_reaction = aliased(Reaction)
    shouts_rating_subq = (
        select(
            Author.id,
            func.coalesce(
                func.sum(
                    case(
                        (shout_reaction.kind == ReactionKind.LIKE.value, 1),
                        (shout_reaction.kind == ReactionKind.DISLIKE.value, -1),
                        else_=0,
                    )
                ),
                0,
            ).label("shouts_rating"),
        )
        .select_from(shout_reaction)
        .outerjoin(Shout, Shout.authors.any(id=Author.id))
        .outerjoin(
            shout_reaction,
            and_(
                shout_reaction.reply_to.is_(None),
                shout_reaction.shout == Shout.id,
                shout_reaction.deleted_at.is_(None),
            ),
        )
        .group_by(Author.id)
        .subquery()
    )

    q = q.outerjoin(shouts_rating_subq, Author.id == shouts_rating_subq.c.id)
    q = q.add_columns(shouts_rating_subq.c.shouts_rating)
    group_list = [shouts_rating_subq.c.shouts_rating]

    # by comments
    replied_comment = aliased(Reaction)
    reaction_2 = aliased(Reaction)
    comments_subq = (
        select(
            Author.id,
            func.coalesce(
                func.sum(
                    case(
                        (reaction_2.kind == ReactionKind.LIKE.value, 1),
                        (reaction_2.kind == ReactionKind.DISLIKE.value, -1),
                        else_=0,
                    )
                ),
                0,
            ).label("comments_rating"),
        )
        .select_from(reaction_2)
        .outerjoin(
            replied_comment,
            and_(
                replied_comment.kind == ReactionKind.COMMENT.value,
                replied_comment.created_by == Author.id,
                reaction_2.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
                reaction_2.reply_to == replied_comment.id,
                reaction_2.deleted_at.is_(None),
            ),
        )
        .group_by(Author.id)
        .subquery()
    )

    q = q.outerjoin(comments_subq, Author.id == comments_subq.c.id)
    q = q.add_columns(comments_subq.c.comments_rating)
    group_list.extend([comments_subq.c.comments_rating])

    return q, group_list
