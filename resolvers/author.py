from typing import List
from datetime import datetime, timedelta, timezone
from sqlalchemy import and_, func, distinct, select, literal
from sqlalchemy.orm import aliased

from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic
from orm.author import AuthorFollower, Author, AuthorRating
from community import followed_communities
from topic import followed_topics
from reaction import load_followed_reactions


def add_author_stat_columns(q):
    followers_table = aliased(AuthorFollower)
    followings_table = aliased(AuthorFollower)
    shout_author_aliased = aliased(ShoutAuthor)
    # author_rating_aliased = aliased(AuthorRating)

    q = q.outerjoin(shout_author_aliased).add_columns(
        func.count(distinct(shout_author_aliased.shout)).label("shouts_stat")
    )
    q = q.outerjoin(followers_table, followers_table.author == Author.id).add_columns(
        func.count(distinct(followers_table.follower)).label("followers_stat")
    )

    q = q.outerjoin(
        followings_table, followings_table.follower == Author.id
    ).add_columns(
        func.count(distinct(followings_table.author)).label("followings_stat")
    )

    q = q.add_columns(literal(0).label("rating_stat"))
    # FIXME
    # q = q.outerjoin(author_rating_aliased, author_rating_aliased.user == Author.id).add_columns(
    #     # TODO: check
    #     func.sum(author_rating_aliased.value).label('rating_stat')
    # )

    q = q.add_columns(literal(0).label("commented_stat"))
    # q = q.outerjoin(Reaction, and_(Reaction.createdBy == Author.id, Reaction.body.is_not(None))).add_columns(
    #     func.count(distinct(Reaction.id)).label('commented_stat')
    # )

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
        # "unread": await get_total_unread_counter(author_id),  # unread inbox messages counter
        "topics": [
            t.slug for t in await followed_topics(author_id)
        ],  # followed topics slugs
        "authors": [
            a.slug for a in await followed_authors(author_id)
        ],  # followed authors slugs
        "reactions": await load_followed_reactions(author_id),
        "communities": [
            c.slug for c in await followed_communities(author_id)
        ],  # communities
    }


@mutation.field("updateProfile")
@login_required
async def update_profile(_, info, profile):
    author_id = info.context["author_id"]
    with local_session() as session:
        author = session.query(Author).where(Author.id == author_id).first()
        author.update(profile)
        session.commit()
        return {"error": None, "author": author}


# for mutation.field("follow")
def author_follow(follower_id, slug):
    try:
        with local_session() as session:
            author = session.query(Author).where(Author.slug == slug).one()
            af = AuthorFollower.create(follower=follower_id, author=author.id)
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


@query.field("authorsAll")
async def get_authors_all(_, _info):
    q = select(Author)
    q = add_author_stat_columns(q)
    q = q.join(ShoutAuthor, Author.id == ShoutAuthor.author)

    return get_authors_from_query(q)


@query.field("getAuthor")
async def get_author(_, _info, slug):
    q = select(Author).where(Author.slug == slug)
    q = add_author_stat_columns(q)

    authors = get_authors_from_query(q)
    return authors[0]


@query.field("loadAuthorsBy")
async def load_authors_by(_, _info, by, limit, offset):
    q = select(Author)
    q = add_author_stat_columns(q)
    if by.get("slug"):
        q = q.filter(Author.slug.ilike(f"%{by['slug']}%"))
    elif by.get("name"):
        q = q.filter(Author.name.ilike(f"%{by['name']}%"))
    elif by.get("topic"):
        q = (
            q.join(ShoutAuthor)
            .join(ShoutTopic)
            .join(Topic)
            .where(Topic.slug == by["topic"])
        )
    if by.get("lastSeen"):  # in days
        days_before = datetime.now(tz=timezone.utc) - timedelta(days=by["lastSeen"])
        q = q.filter(Author.lastSeen > days_before)
    elif by.get("createdAt"):  # in days
        days_before = datetime.now(tz=timezone.utc) - timedelta(days=by["createdAt"])
        q = q.filter(Author.createdAt > days_before)

    q = q.order_by(by.get("order", Author.createdAt)).limit(limit).offset(offset)

    return get_authors_from_query(q)


async def get_followed_authors(_, _info, slug) -> List[Author]:
    # First, we need to get the author_id for the given slug
    with local_session() as session:
        author_id_query = select(Author.id).where(Author.slug == slug)
        author_id = session.execute(author_id_query).scalar()

    if author_id is None:
        raise ValueError("Author not found")

    return await followed_authors(author_id)


async def author_followers(_, _info, slug) -> List[Author]:
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
    q = q.join(AuthorFollower, AuthorFollower.author == Author.id).where(
        AuthorFollower.follower == follower_id
    )
    # Pass the query to the get_authors_from_query function and return the results
    return get_authors_from_query(q)


@mutation.field("rateAuthor")
@login_required
async def rate_author(_, info, rated_user_id, value):
    author_id = info.context["author_id"]

    with local_session() as session:
        rating = (
            session.query(AuthorRating)
            .filter(
                and_(
                    AuthorRating.rater == author_id, AuthorRating.user == rated_user_id
                )
            )
            .first()
        )
        if rating:
            rating.value = value
            session.commit()
            return {}
    try:
        AuthorRating.create(rater=author_id, user=rated_user_id, value=value)
    except Exception as err:
        return {"error": err}
    return {}
