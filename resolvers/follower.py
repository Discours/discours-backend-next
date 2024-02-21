from typing import List

from sqlalchemy.orm import aliased
from sqlalchemy.sql import and_

from orm.author import Author, AuthorFollower
from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.community import community_follow, community_unfollow
from resolvers.topic import topic_follow, topic_unfollow
from services.auth import login_required
from services.db import local_session
from services.notify import notify_follower
from services.schema import mutation, query
from services.logger import root_logger as logger
from services.rediscache import redis


@mutation.field("follow")
@login_required
async def follow(_, info, what, slug):
    try:
        user_id = info.context["user_id"]
        with local_session() as session:
            actor = session.query(Author).filter(Author.user == user_id).first()
            if actor:
                follower_id = actor.id
                if what == "AUTHOR":
                    if author_follow(follower_id, slug):
                        author = (
                            session.query(Author.id).where(Author.slug == slug).one()
                        )
                        follower = (
                            session.query(Author).where(Author.id == follower_id).one()
                        )
                        await notify_follower(follower.dict(), author.id)
                elif what == "TOPIC":
                    topic_follow(follower_id, slug)
                elif what == "COMMUNITY":
                    community_follow(follower_id, slug)
                elif what == "REACTIONS":
                    reactions_follow(follower_id, slug)
    except Exception as e:
        logger.debug(info, what, slug)
        logger.error(e)
        return {"error": str(e)}

    return {}


@mutation.field("unfollow")
@login_required
async def unfollow(_, info, what, slug):
    user_id = info.context["user_id"]
    try:
        with local_session() as session:
            actor = session.query(Author).filter(Author.user == user_id).first()
            if actor:
                follower_id = actor.id
                if what == "AUTHOR":
                    if author_unfollow(follower_id, slug):
                        author = (
                            session.query(Author.id).where(Author.slug == slug).one()
                        )
                        follower = (
                            session.query(Author).where(Author.id == follower_id).one()
                        )
                        await notify_follower(follower.dict(), author.id, "unfollow")
                elif what == "TOPIC":
                    topic_unfollow(follower_id, slug)
                elif what == "COMMUNITY":
                    community_unfollow(follower_id, slug)
                elif what == "REACTIONS":
                    reactions_unfollow(follower_id, slug)
    except Exception as e:
        return {"error": str(e)}

    return {}


def query_follows(user_id: str):
    topics = set()
    authors = set()
    communities = []
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if isinstance(author, Author):
            author_id = author.id
            aliased_author = aliased(Author)
            authors_query = (
                session.query(aliased_author, AuthorFollower)
                .join(AuthorFollower, AuthorFollower.follower == author_id)
                .filter(AuthorFollower.author == aliased_author.id)
            )

            topics_query = (
                session.query(Topic, TopicFollower)
                .join(TopicFollower, TopicFollower.follower == author_id)
                .filter(TopicFollower.topic == Topic.id)
            )

            shouts_query = (
                session.query(Shout, ShoutReactionsFollower)
                .join(ShoutReactionsFollower, ShoutReactionsFollower.follower == author_id)
                .filter(ShoutReactionsFollower.shout == Shout.id)
            )

            authors = set(session.execute(authors_query).scalars())
            topics = set(session.execute(topics_query).scalars())
            shouts = set(session.execute(shouts_query).scalars())
            communities = session.query(Community).all()

    return {
        "topics": list(topics),
        "authors": list(authors),
        "shouts": list(shouts),
        "communities": communities,
    }


async def get_follows_by_user_id(user_id: str):
    redis_key = f"user:{user_id}:follows"
    res = await redis.execute("HGET", redis_key)
    if res:
        return res

    logger.info(f"getting follows for {user_id}")
    follows = query_follows(user_id)
    await redis.execute("HSET", redis_key, follows)

    return follows


@query.field("get_my_followed")
@login_required
async def get_my_followed(_, info):
    user_id = info.context["user_id"]
    return await get_follows_by_user_id(user_id)


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
                return True
    except Exception:
        return False


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
                return True
    except Exception as ex:
        logger.debug(ex)
    return False


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
