from sqlalchemy import and_, func, case, true, select, distinct
from sqlalchemy.orm import aliased

from orm.author import AuthorRating, Author
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout
from services.auth import login_required
from services.db import local_session
from services.schema import mutation


@mutation.field('rate_author')
@login_required
async def rate_author(_, info, rated_slug, value):
    user_id = info.context['user_id']

    with local_session() as session:
        rated_author = session.query(Author).filter(Author.slug == rated_slug).first()
        rater = session.query(Author).filter(Author.slug == user_id).first()
        if rater and rated_author:
            rating: AuthorRating = (
                session.query(AuthorRating)
                .filter(
                    and_(
                        AuthorRating.rater == rater.id,
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
                    rating = AuthorRating(
                        rater=rater.id, author=rated_author.id, plus=value > 0
                    )
                    session.add(rating)
                    session.commit()
                except Exception as err:
                    return {'error': err}
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


def load_author_ratings(session, author: Author):
    comments_count = (
        session.query(Reaction)
        .filter(
            and_(
                Reaction.created_by == author.id,
                Reaction.kind == ReactionKind.COMMENT.value,
                Reaction.deleted_at.is_(None),
            )
        )
        .count()
    )
    likes_count = (
        session.query(AuthorRating)
        .filter(and_(AuthorRating.author == author.id, AuthorRating.plus.is_(True)))
        .count()
    )
    dislikes_count = (
        session.query(AuthorRating)
        .filter(and_(AuthorRating.author == author.id, AuthorRating.plus.is_not(True)))
        .count()
    )
    author.stat = author.stat if isinstance(author.stat, dict) else {}
    author.stat['rating'] = likes_count - dislikes_count
    author.stat['rating_shouts'] = count_author_shouts_rating(session, author.id)
    author.stat['rating_comments'] = count_author_comments_rating(session, author.id)
    author.stat['commented'] = comments_count
    return author


def add_rating_columns(q, group_list):
    # old karma
    q = q.outerjoin(AuthorRating, AuthorRating.author == Author.id)
    q = q.add_columns(
        func.sum(case((AuthorRating.plus == true(), 1), else_=0)).label('likes_count'),
        func.sum(case((AuthorRating.plus != true(), 1), else_=0)).label('dislikes_count'),
    )

    # by shouts
    shouts_subq = select(Reaction).where(
        and_(
            Reaction.shout == Shout.id,
            Shout.authors.any(id=Author.id),
            Reaction.reply_to.is_(None),
            Reaction.deleted_at.is_(None)
        )
    ).subquery()

    shouts_subq_alias = aliased(shouts_subq)
    q = q.outerjoin(shouts_subq_alias, Author.id == shouts_subq_alias.c.author)
    q = q.add_columns(
        func.count(distinct(case((shouts_subq.c.kind == ReactionKind.LIKE.value, 1)))).label('shouts_likes'),
        func.count(distinct(case((shouts_subq.c.kind == ReactionKind.DISLIKE.value, 1)))).label('shouts_dislikes'),
    )
    group_list.extend([shouts_subq_alias.c.shouts_likes, shouts_subq_alias.c.shouts_dislikes])

    # by comments
    replied_comment = aliased(Reaction)
    comments_subq = select(Reaction).where(
        and_(
            replied_comment.kind == ReactionKind.COMMENT.value,
            replied_comment.created_by == Author.id,
            Reaction.reply_to == replied_comment.id,
            Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
            Reaction.deleted_at.is_(None)
        )
    ).subquery()

    q = q.outerjoin(comments_subq, comments_subq.c.reply_to == replied_comment.id)
    q = q.add_columns(
        func.count(distinct(case((comments_subq.c.kind == ReactionKind.LIKE.value, 1)))).label('comments_likes'),
        func.count(distinct(case((comments_subq.c.kind == ReactionKind.DISLIKE.value, 1)))).label('comments_dislikes'),
    )
    group_list.extend([comments_subq.c.comments_likes, comments_subq.c.comments_dislikes])

    return q, group_list
