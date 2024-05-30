from typing import List

from sqlalchemy import select
from sqlalchemy.sql import and_

from orm.author import Author, AuthorFollower
from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.cache import (
    cache_author,
    cache_topic,
    get_cached_author_by_user_id,
    get_cached_follower_authors,
    get_cached_follower_topics,
)
from services.db import local_session
from services.logger import root_logger as logger
from services.notify import notify_follower
from services.schema import mutation, query


async def cache_by_slug(what: str, slug: str):
    is_author = what == "AUTHOR"
    alias = Author if is_author else Topic
    caching_query = select(alias).filter(alias.slug == slug)
    [x] = get_with_stat(caching_query)
    if not x:
        return

    d = x.dict()  # convert object to dictionary
    if is_author:
        await cache_author(d)
    else:
        await cache_topic(d)
    return d


@mutation.field("follow")
@login_required
async def follow(_, info, what, slug):
    error = None
    user_id = info.context.get("user_id")
    follower_dict = info.context.get("author")
    if not user_id or not follower_dict:
        return {"error": "unauthorized"}
    follower_id = follower_dict.get("id")
    entity = what.lower()

    if what == "AUTHOR":
        follower_id = int(follower_id)
        error = author_follow(follower_id, slug)
        if not error:
            follows = await get_cached_follower_authors(follower_id)
            with local_session() as session:
                [author_id] = session.query(Author.id).filter(Author.slug == slug).first()
                if author_id and author_id not in follows:
                    follows.append(author_id)
                    await cache_author(follower_dict)
                    await notify_follower(follower_dict, author_id, "follow")
                    [author] = get_with_stat(select(Author).filter(Author.id == author_id))
                    if author:
                        author_dict = author.dict()
                        await cache_author(author_dict)

    elif what == "TOPIC":
        error = topic_follow(follower_id, slug)
        if not error:
            follows = await get_cached_follower_topics(follower_id)
            topic_dict = await cache_by_slug(what, slug)
            await cache_topic(topic_dict)

    elif what == "COMMUNITY":
        with local_session() as session:
            follows = session.query(Community).all()

    elif what == "SHOUT":
        error = reactions_follow(follower_id, slug)
        if not error:
            # TODO: follows = await get_cached_follower_reactions(follower_id)
            # shout_dict = await cache_shout_by_slug(what, slug)
            # await cache_topic(topic_dict)
            pass

    return {f"{entity}s": follows, "error": error}


@mutation.field("unfollow")
@login_required
async def unfollow(_, info, what, slug):
    follows = []
    error = None
    user_id = info.context.get("user_id")
    follower_dict = info.context.get("author")
    if not user_id or not follower_dict:
        return {"error": "unauthorized"}
    follower_id = follower_dict.get("id")

    entity = what.lower()
    follows = []

    if what == "AUTHOR":
        follows = await get_cached_follower_authors(follower_id)
        follower_id = int(follower_id)
        error = author_unfollow(follower_id, slug)
        # NOTE: after triggers should update cached stats
        if not error:
            logger.info(f"@{follower_dict.get('slug')} unfollowed @{slug}")
            [author_id] = local_session().query(Author.id).filter(Author.slug == slug).first()
            if author_id and author_id in follows:
                follows.remove(author_id)
                await cache_author(follower_dict)
                await notify_follower(follower_dict, author_id, "follow")
                [author] = get_with_stat(select(Author).filter(Author.id == author_id))
                if author:
                    author_dict = author.dict()
                    await cache_author(author_dict)

    elif what == "TOPIC":
        error = topic_unfollow(follower_id, slug)
        if not error:
            follows = await get_cached_follower_topics(follower_id)
            topic_dict = await cache_by_slug(what, slug)
            await cache_topic(topic_dict)

    elif what == "COMMUNITY":
        with local_session() as session:
            follows = session.execute(select(Community))

    elif what == "SHOUT":
        error = reactions_unfollow(follower_id, slug)
        if not error:
            pass

    return {"error": error, f"{entity}s": follows}


async def get_follows_by_user_id(user_id: str):
    if not user_id:
        return {"error": "unauthorized"}
    author = await get_cached_author_by_user_id(user_id, get_with_stat)
    if not author:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            if not author:
                return {"error": "cant find author"}
            author = author.dict()

    author_id = author.get("id")
    if author_id:
        topics = await get_cached_follower_topics(author_id)
        authors = await get_cached_follower_authors(author_id)
    return {
        "topics": topics or [],
        "authors": authors or [],
        "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
    }


def topic_follow(follower_id, slug):
    try:
        with local_session() as session:
            topic = session.query(Topic).where(Topic.slug == slug).one()
            _following = TopicFollower(topic=topic.id, follower=follower_id)
        return None
    except Exception as error:
        logger.warn(error)
        return "cant follow"


def topic_unfollow(follower_id, slug):
    try:
        with local_session() as session:
            sub = (
                session.query(TopicFollower)
                .join(Topic)
                .filter(and_(TopicFollower.follower == follower_id, Topic.slug == slug))
                .first()
            )
            if sub:
                session.delete(sub)
                session.commit()
        return None
    except Exception as error:
        logger.warn(error)
        return "cant unfollow"


def reactions_follow(author_id, shout_id, auto=False):
    try:
        with local_session() as session:
            shout = session.query(Shout).where(Shout.id == shout_id).one()

            following = (
                session.query(ShoutReactionsFollower)
                .where(
                    and_(
                        ShoutReactionsFollower.follower == author_id,
                        ShoutReactionsFollower.shout == shout.id,
                    )
                )
                .first()
            )

            if not following:
                following = ShoutReactionsFollower(follower=author_id, shout=shout.id, auto=auto)
                session.add(following)
                session.commit()
        return None
    except Exception as error:
        logger.warn(error)
        return "cant follow"


def reactions_unfollow(author_id, shout_id: int):
    try:
        with local_session() as session:
            shout = session.query(Shout).where(Shout.id == shout_id).one()

            following = (
                session.query(ShoutReactionsFollower)
                .where(
                    and_(
                        ShoutReactionsFollower.follower == author_id,
                        ShoutReactionsFollower.shout == shout.id,
                    )
                )
                .first()
            )

            if following:
                session.delete(following)
                session.commit()
        return None
    except Exception as error:
        logger.warn(error)
        return "cant unfollow"


# for mutation.field("follow")
def author_follow(follower_id, slug):
    try:
        with local_session() as session:
            author = session.query(Author).where(Author.slug == slug).one()
            af = AuthorFollower(follower=follower_id, author=author.id)
            session.add(af)
            session.commit()
        return None
    except Exception as error:
        logger.warn(error)
        return "cant follow"


# for mutation.field("unfollow")
def author_unfollow(follower_id, slug):
    try:
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
                return None
    except Exception as error:
        logger.warn(error)
        return "cant unfollow"


@query.field("get_shout_followers")
def get_shout_followers(_, _info, slug: str = "", shout_id: int | None = None) -> List[Author]:
    followers = []
    with local_session() as session:
        shout = None
        if slug:
            shout = session.query(Shout).filter(Shout.slug == slug).first()
        elif shout_id:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
        if shout:
            reactions = session.query(Reaction).filter(Reaction.shout == shout.id).all()
            for r in reactions:
                followers.append(r.created_by)

    return followers
