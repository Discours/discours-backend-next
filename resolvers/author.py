import time
from typing import List

from sqlalchemy import and_, case, distinct, func, literal, select, cast, Integer
from sqlalchemy.orm import aliased

from orm.author import Author, AuthorFollower, AuthorRating
from orm.community import Community
from orm.reaction import Reaction, ReactionKind
from orm.shout import ShoutAuthor, ShoutTopic, Shout
from orm.topic import Topic
from resolvers.community import followed_communities
from resolvers.reaction import reacted_shouts_updates as followed_reactions
from resolvers.topic import followed_topics
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from services.unread import get_total_unread_counter


def add_author_stat_columns(q):
    shout_author_aliased = aliased(ShoutAuthor)
    q = q.outerjoin(shout_author_aliased, shout_author_aliased.author == Author.id).add_columns(
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

    q = q.group_by(Author.id)
    return q


def get_authors_from_query(q):
    authors = []
    with local_session() as session:
        for [author, shouts_stat, followers_stat, followings_stat] in session.execute(q):
            author.stat = {
                "shouts": shouts_stat,
                "followers": followers_stat,
                "followings": followings_stat,
            }
            authors.append(author)
    # print(f"[resolvers.author] get_authors_from_query {authors}")
    return authors


async def author_followings(author_id: int):
    # NOTE: topics, authors, shout-reactions and communities slugs list
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


# TODO: caching query
@query.field("get_authors_all")
async def get_authors_all(_, _info):
    with local_session() as session:
        return session.query(Author).all()


def count_author_comments_rating(session, author_id) -> int:
    replied_alias = aliased(Reaction)
    replies_likes = (
        session.query(replied_alias)
        .join(Reaction, replied_alias.id == Reaction.reply_to)
        .where(and_(replied_alias.created_by == author_id, replied_alias.kind == ReactionKind.COMMENT.value))
        .filter(replied_alias.kind == ReactionKind.LIKE.value)
        .count()
    ) or 0
    replies_dislikes = (
        session.query(replied_alias)
        .join(Reaction, replied_alias.id == Reaction.reply_to)
        .where(and_(replied_alias.created_by == author_id, replied_alias.kind == ReactionKind.COMMENT.value))
        .filter(replied_alias.kind == ReactionKind.DISLIKE.value)
        .count()
    ) or 0

    return  replies_likes - replies_dislikes


def count_author_shouts_rating(session, author_id) -> int:
    shouts_likes = (
        session.query(Reaction, Shout)
        .join(Shout, Shout.id == Reaction.shout)
        .filter(and_(Shout.authors.any(id=author_id), Reaction.kind == ReactionKind.LIKE.value))
        .count()
        or 0
    )
    shouts_dislikes = (
        session.query(Reaction, Shout)
        .join(Shout, Shout.id == Reaction.shout)
        .filter(and_(Shout.authors.any(id=author_id), Reaction.kind == ReactionKind.DISLIKE.value))
        .count()
        or 0
    )
    return shouts_likes - shouts_dislikes



def load_author_with_stats(q):
    q = add_author_stat_columns(q)

    [author] = get_authors_from_query(q)

    if author:
        with local_session() as session:
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
            ratings_sum = (
                session.query(
                    func.sum(
                        case((AuthorRating.plus == True, cast(1, Integer)),
                            else_=cast(-1, Integer))).label("rating")
                )
                .filter(AuthorRating.author == author.id)
                .scalar()
            )

            author.stat["rating"] = ratings_sum or 0
            author.stat["rating_shouts"] = count_author_shouts_rating(session, author.id)
            author.stat["rating_comments"] = count_author_comments_rating(session, author.id)
            author.stat["commented"] = comments_count
            return author


@query.field("get_author")
async def get_author(_, _info, slug="", author_id=None):
    q = None
    if slug or author_id:
        if bool(slug):
            q = select(Author).where(Author.slug == slug)
        if author_id:
            q = select(Author).where(Author.id == author_id)

        return load_author_with_stats(q)


@query.field("get_author_id")
async def get_author_id(_, _info, user: str):
    with local_session() as session:
        print(f"[resolvers.author] getting author id for {user}")
        q = select(Author).filter(Author.user == user)
        return load_author_with_stats(q)


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
    author_id_query = None
    if slug:
        author_id_query = select(Author.id).where(Author.slug == slug)
    elif user:
        author_id_query = select(Author.id).where(Author.user == user)
    if author_id_query is not None and not author_id:
        with local_session() as session:
            author_id = session.execute(author_id_query).scalar()

    if author_id is None:
        raise ValueError("Author not found")
    else:
        return await followed_authors(author_id)  # Author[]


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
            rating: AuthorRating = (
                session.query(AuthorRating)
                .filter(and_(AuthorRating.rater == rater.id, AuthorRating.author == rated_author.id))
                .first()
            )
            if rating:
                rating.plus = value > 0
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


async def create_author(user_id: str, slug: str, name: str = ""):
    with local_session() as session:
        new_author = Author(user=user_id, slug=slug, name=name)
        session.add(new_author)
        session.commit()
        print(f"[resolvers.author] created by webhook {new_author.dict()}")
