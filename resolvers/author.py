import time
from typing import List
from sqlalchemy import and_, func, distinct, select, literal, case
from sqlalchemy.orm import aliased

from orm.reaction import Reaction, ReactionKind
from services.auth import login_required
from services.db import local_session
from services.unread import get_total_unread_counter
from services.schema import mutation, query
from orm.community import Community
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic
from orm.author import AuthorFollower, Author, AuthorRating
from resolvers.community import followed_communities
from resolvers.topic import followed_topics
from resolvers.reaction import reacted_shouts_updates as followed_reactions


def add_author_stat_columns(q):
    shout_author_aliased = aliased(ShoutAuthor)
    q = q.outerjoin(shout_author_aliased).add_columns(
        func.count(distinct(shout_author_aliased.shout)).label("shouts_stat")
    )

    followers_table = aliased(AuthorFollower)
    q = q.outerjoin(followers_table, followers_table.author == Author.id).add_columns(
        func.count(distinct(followers_table.follower)).label("followers_stat")
    )

    followings_table = aliased(AuthorFollower)
    q = q.outerjoin(followings_table, followings_table.follower == Author.id).add_columns(
        func.count(distinct(followers_table.author)).label("followings_stat")
    )

    rating_aliased = aliased(Reaction)
    # q = q.add_columns(literal(0).label("rating_stat"))
    q = q.outerjoin(rating_aliased, rating_aliased.shout == shout_author_aliased.shout).add_columns(
        func.coalesce(
            func.sum(
                case(
                    (and_(rating_aliased.kind == ReactionKind.LIKE.value, rating_aliased.reply_to.is_(None)), 1),
                    (and_(rating_aliased.kind == ReactionKind.DISLIKE.value, rating_aliased.reply_to.is_(None)), -1),
                    else_=0,
                )
            ),
            0,
        ).label("rating_stat")
    )

    q = q.add_columns(literal(0).label("commented_stat"))

    # WARNING: too high cpu cost

    # TODO: check version 1
    # q = q.outerjoin(
    #     Reaction, and_(Reaction.createdBy == User.id, Reaction.body.is_not(None))
    # ).add_columns(func.count(distinct(Reaction.id))
    # .label("commented_stat"))

    # TODO: check version 2
    # q = q.add_columns(
    #   func.count(case((reaction_aliased.kind == ReactionKind.COMMENT.value, 1), else_=0))
    #   .label("commented_stat"))

    # Filter based on shouts where the user is the author
    q = q.filter(shout_author_aliased.author == Author.id)

    q = q.group_by(Author.id)

    return q


def add_stat(author, stat_columns):
    [
        shouts_stat,
        followers_stat,
        followings_stat,
        rating_stat,
        commented_stat,
    ] = stat_columns
    author.stat = {
        "shouts": shouts_stat,
        "followers": followers_stat,
        "followings": followings_stat,
        "rating": rating_stat,
        "commented": commented_stat,
    }

    return author


def get_authors_from_query(q):
    authors = []
    with local_session() as session:
        for [author, *stat_columns] in session.execute(q):
            author = add_stat(author, stat_columns)
            authors.append(author)

    return authors


async def author_followings(author_id: int):
    return {
        "unread": await get_total_unread_counter(author_id),
        "topics": [t.slug for t in await followed_topics(author_id)],
        "authors": [a.slug for a in await followed_authors(author_id)],
        "reactions": [s.slug for s in followed_reactions(author_id)],
        "communities": [c.slug for c in [followed_communities(author_id)] if isinstance(c, Community)],
    }


@mutation.field("update_profile")
@login_required
async def update_profile(_, info, profile):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).where(Author.user == user_id).first()
        Author.update(author, profile)
        session.add(author)
        session.commit()
        return {"error": None, "author": author}


# for mutation.field("follow")
def author_follow(follower_id, slug):
    try:
        with local_session() as session:
            author = session.query(Author).where(Author.slug == slug).one()
            af = AuthorFollower(follower=follower_id, author=author.id)
            session.add(af)
            session.commit()
        return True
    except Exception:
        return False


# for mutation.field("unfollow")
def author_unfollow(follower_id, slug):
    with local_session() as session:
        flw = (
            session.query(AuthorFollower)
            .join(Author, Author.id == AuthorFollower.author)
            .filter(and_(AuthorFollower.follower == follower_id, Author.slug == slug))
            .first()
        )
        if flw:
            session.delete(flw)
            session.commit()
            return True
    return False


@query.field("get_authors_all")
async def get_authors_all(_, _info):
    q = select(Author)
    q = add_author_stat_columns(q)
    q = q.join(ShoutAuthor, Author.id == ShoutAuthor.author)

    return get_authors_from_query(q)


@query.field("get_author")
async def get_author(_, _info, slug="", user=None, author_id=None):
    q = None
    if slug or user or author_id:
        if slug:
            q = select(Author).where(Author.slug == slug)
        elif user:
            q = select(Author).where(Author.user == user)
        elif author_id:
            q = select(Author).where(Author.id == author_id)
        q = add_author_stat_columns(q)

        authors = get_authors_from_query(q)
        return authors[0]


@query.field("load_authors_by")
async def load_authors_by(_, _info, by, limit, offset):
    q = select(Author)
    q = add_author_stat_columns(q)
    if by.get("slug"):
        q = q.filter(Author.slug.ilike(f"%{by['slug']}%"))
    elif by.get("name"):
        q = q.filter(Author.name.ilike(f"%{by['name']}%"))
    elif by.get("topic"):
        q = q.join(ShoutAuthor).join(ShoutTopic).join(Topic).where(Topic.slug == by["topic"])

    if by.get("last_seen"):  # in unixtime
        before = int(time.time()) - by["last_seen"]
        q = q.filter(Author.last_seen > before)
    elif by.get("created_at"):  # in unixtime
        before = int(time.time()) - by["created_at"]
        q = q.filter(Author.created_at > before)

    q = q.order_by(by.get("order", Author.created_at)).limit(limit).offset(offset)

    return get_authors_from_query(q)


@query.field("get_author_followed")
async def get_author_followed(_, _info, slug="", user=None, author_id=None) -> List[Author]:
    # First, we need to get the author_id for the given slug
    with local_session() as session:
        author_id_query = select(Author.id).where(Author.slug == slug)
        author_id = session.execute(author_id_query).scalar()

    if author_id is None:
        raise ValueError("Author not found")

    return await followed_authors(author_id)


@query.field("get_author_followers")
async def get_author_followers(_, _info, slug) -> List[Author]:
    q = select(Author)
    q = add_author_stat_columns(q)

    aliased_author = aliased(Author)
    q = (
        q.join(AuthorFollower, AuthorFollower.follower == Author.id)
        .join(aliased_author, aliased_author.id == AuthorFollower.author)
        .where(aliased_author.slug == slug)
    )

    return get_authors_from_query(q)


async def followed_authors(follower_id):
    q = select(Author)
    q = add_author_stat_columns(q)
    q = q.join(AuthorFollower, AuthorFollower.author == Author.id).where(AuthorFollower.follower == follower_id)
    # Pass the query to the get_authors_from_query function and return the results
    return get_authors_from_query(q)


@mutation.field("rate_author")
@login_required
async def rate_author(_, info, rated_slug, value):
    user_id = info.context["user_id"]

    with local_session() as session:
        rated_author = session.query(Author).filter(Author.slug == rated_slug).first()
        rater = session.query(Author).filter(Author.slug == user_id).first()
        if rater and rated_author:
            rating = (
                session.query(AuthorRating)
                .filter(and_(AuthorRating.rater == rater.id, AuthorRating.author == rated_author.id))
                .first()
            )
            if value > 0:
                rating.plus = True
                session.add(rating)
                session.commit()
                return {}
            else:
                try:
                    rating = AuthorRating(rater=rater.id, author=rated_author.id, plus=value > 0)
                    session.add(rating)
                    session.commit()
                except Exception as err:
                    return {"error": err}
    return {}


async def create_author(user_id: str, slug: str):
    with local_session() as session:
        new_author = Author(user=user_id, slug=slug)
        session.add(new_author)
        session.commit()
