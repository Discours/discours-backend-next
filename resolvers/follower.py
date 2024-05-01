import json
import time
from typing import List

from psycopg2.errors import UniqueViolation
from sqlalchemy import or_, select
from sqlalchemy.sql import and_

from orm.author import Author, AuthorFollower
from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import author_follows_authors, author_follows_topics, get_with_stat
from services.auth import login_required
from services.cache import DEFAULT_FOLLOWS, cache_follower
from services.db import local_session
from services.logger import root_logger as logger
from services.notify import notify_follower
from services.rediscache import redis
from services.schema import mutation, query


@mutation.field("follow")
@login_required
async def follow(_, info, what, slug):
    error = None
    user_id = info.context.get("user_id")
    follower_dict = info.context["author"]
    follower_id = follower_dict.get("id")
    if not user_id or not follower_id:
        return {"error": "unauthorized"}

    entity = what.lower()
    follows = []
    follows_str = await redis.execute("GET", f"author:{follower_id}:follows-{entity}s")
    if isinstance(follows_str, str):
        follows = json.loads(follows_str)

    if not follows:
        return {"error": "cant find following cache"}

    if what == "AUTHOR":
        follower_id = int(follower_id)
        error = author_follow(follower_id, slug)
        if not error:
            result = get_with_stat(select(Author).where(Author.slug == slug))
            if result:
                [author] = result
                if author:
                    author_dict = author.dict()
                    await cache_follower(follower_dict, author_dict)
                    await notify_follower(follower_dict, author.id, "follow")
                    if not any(a["id"] == author.id for a in follows):
                        if author_dict not in follows:
                            follows.append(author_dict)

    elif what == "TOPIC":
        error = topic_follow(follower_id, slug)

    elif what == "COMMUNITY":
        # FIXME: when more communities
        follows = local_session().execute(select(Community))

    elif what == "SHOUT":
        error = reactions_follow(follower_id, slug)

    if error:
        return {"error": error}

    return {f"{entity}s": follows}


@mutation.field("unfollow")
@login_required
async def unfollow(_, info, what, slug):
    follows = []
    error = None
    user_id = info.context.get("user_id")
    follower_dict = info.context["author"]
    follower_id = follower_dict.get("id")
    if not user_id:
        return {"error": "unauthorized"}

    if not follower_id:
        return {"error": "cant find follower account"}

    if what == "AUTHOR":
        error = author_unfollow(follower_id, slug)
        # NOTE: after triggers should update cached stats
        if not error:
            logger.info(f"@{follower_dict.get('slug')} unfollowed @{slug}")
            author = local_session().query(Author).where(Author.slug == slug).first()
            if isinstance(author, Author):
                author_dict = author.dict()
                await cache_follower(follower_dict, author_dict, False)
                await notify_follower(follower_dict, author.id, "unfollow")
                for idx, item in enumerate(follows):
                    if item["id"] == author.id:
                        follows.pop(idx)  # Remove the author_dict from the follows list
                        break

    elif what == "TOPIC":
        error = topic_unfollow(follower_id, slug)

    elif what == "COMMUNITY":
        follows = local_session().execute(select(Community))

    elif what == "SHOUT":
        error = reactions_unfollow(follower_id, slug)

    entity = what.lower()
    follows_str = await redis.execute("GET", f"author:{follower_id}:follows-{entity}s")
    if isinstance(follows_str, str):
        follows = json.loads(follows_str)
    return {"error": error, f"{entity}s": follows}


async def get_follows_by_user_id(user_id: str):
    if not user_id:
        return {"error": "unauthorized"}
    author = await redis.execute("GET", f"user:{user_id}")
    if isinstance(author, str):
        author = json.loads(author)
    if not author:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            if not author:
                return {"error": "cant find author"}
            author = author.dict()
    last_seen = author.get("last_seen", 0) if isinstance(author, dict) else 0
    follows = DEFAULT_FOLLOWS
    day_old = int(time.time()) - last_seen > 24 * 60 * 60
    if day_old:
        author_id = json.loads(str(author)).get("id")
        if author_id:
            topics = author_follows_topics(author_id)
            authors = author_follows_authors(author_id)
            follows = {
                "topics": topics,
                "authors": authors,
                "communities": [
                    {"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}
                ],
            }
    else:
        logger.debug(f"getting follows for {user_id} from redis")
        res = await redis.execute("GET", f"user:{user_id}:follows")
        if isinstance(res, str):
            follows = json.loads(res)
    return follows


def topic_follow(follower_id, slug):
    try:
        with local_session() as session:
            topic = session.query(Topic).where(Topic.slug == slug).one()
            _following = TopicFollower(topic=topic.id, follower=follower_id)
        return None
    except UniqueViolation as error:
        logger.warn(error)
        return "already followed"
    except Exception as exc:
        logger.error(exc)
        return exc


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
    except UniqueViolation as error:
        logger.warn(error)
        return "already unfollowed"
    except Exception as ex:
        logger.debug(ex)
        return ex


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
                following = ShoutReactionsFollower(
                    follower=author_id, shout=shout.id, auto=auto
                )
                session.add(following)
                session.commit()
        return None
    except UniqueViolation as error:
        logger.warn(error)
        return "already followed"
    except Exception as exc:
        return exc


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
    except UniqueViolation as error:
        logger.warn(error)
        return "already unfollowed"
    except Exception as ex:
        import traceback

        traceback.print_exc()
        return ex


# for mutation.field("follow")
def author_follow(follower_id, slug):
    try:
        with local_session() as session:
            author = session.query(Author).where(Author.slug == slug).one()
            af = AuthorFollower(follower=follower_id, author=author.id)
            session.add(af)
            session.commit()
        return None
    except UniqueViolation as error:
        logger.warn(error)
        return "already followed"
    except Exception as exc:
        import traceback

        traceback.print_exc()
        return exc


# for mutation.field("unfollow")
def author_unfollow(follower_id, slug):
    try:
        with local_session() as session:
            flw = (
                session.query(AuthorFollower)
                .join(Author, Author.id == AuthorFollower.author)
                .filter(
                    and_(AuthorFollower.follower == follower_id, Author.slug == slug)
                )
                .first()
            )
            if flw:
                session.delete(flw)
                session.commit()
                return None
    except UniqueViolation as error:
        logger.warn(error)
        return "already unfollowed"
    except Exception as exc:
        return exc


@query.field("get_topic_followers")
async def get_topic_followers(_, _info, slug: str, topic_id: int) -> List[Author]:
    q = select(Author)
    q = (
        q.join(TopicFollower, TopicFollower.follower == Author.id)
        .join(Topic, Topic.id == TopicFollower.topic)
        .filter(or_(Topic.slug == slug, Topic.id == topic_id))
    )
    return get_with_stat(q)


@query.field("get_shout_followers")
def get_shout_followers(
    _, _info, slug: str = "", shout_id: int | None = None
) -> List[Author]:
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
