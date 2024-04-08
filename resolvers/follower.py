import json
import time
from typing import List

from psycopg2.errors import UniqueViolation
from sqlalchemy import or_, select
from sqlalchemy.sql import and_

from orm.author import Author, AuthorFollower
from orm.community import Community
# from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import (author_follows_authors, author_follows_topics,
                            get_authors_with_stat_cached,
                            get_topics_with_stat_cached)
from services.auth import login_required
from services.cache import (DEFAULT_FOLLOWS, update_followers_for_author,
                            update_follows_for_author)
from services.db import local_session
from services.logger import root_logger as logger
from services.notify import notify_follower
from services.rediscache import redis
from services.schema import mutation, query


@mutation.field('follow')
@login_required
async def follow(_, info, what, slug):
    follows = []
    error = None
    user_id = info.context.get('user_id')
    if not user_id:
        return {'error': 'unauthorized'}
    [follower] = await get_authors_with_stat_cached(
        select(Author).select_from(Author).filter(Author.user == user_id)
    )
    if not follower:
        return {'error': 'cant find follower'}

    if what == 'AUTHOR':
        error = author_follow(follower.id, slug)
        if not error:
            logger.debug(f'@{follower.slug} followed @{slug}')
            [author] = await get_authors_with_stat_cached(
                select(Author).select_from(Author).where(Author.slug == slug)
            )
            if not author:
                return {'error': 'author is not found'}
            follows = await update_follows_for_author(
                follower, 'author', author.dict(), True
            )
            _followers = await update_followers_for_author(follower, author, True)
            await notify_follower(follower.dict(), author.id, 'unfollow')

    elif what == 'TOPIC':
        error = topic_follow(follower.id, slug)
        if not error:
            [topic] = await get_topics_with_stat_cached(
                select(Topic).where(Topic.slug == slug)
            )
            if not topic:
                return {'error': 'topic is not found'}
            follows = await update_follows_for_author(
                follower, 'topic', topic.dict(), True
            )

    elif what == 'COMMUNITY':
        follows = local_session().execute(select(Community))

    elif what == 'SHOUT':
        error = reactions_follow(follower.id, slug)
        if not error:
            [shout] = local_session().execute(select(Shout).where(Shout.slug == slug))
            if not shout:
                return {'error': 'cant find shout'}
            follows = await update_follows_for_author(
                follower, 'shout', shout.dict(), True
            )

    return {f'{what.lower()}s': follows, 'error': error}


@mutation.field('unfollow')
@login_required
async def unfollow(_, info, what, slug):
    follows = []
    error = None
    user_id = info.context.get('user_id')
    if not user_id:
        return {'error': 'unauthorized'}
    follower_query = select(Author).filter(Author.user == user_id)
    [follower] = await get_authors_with_stat_cached(follower_query)
    if not follower:
        return {'error': 'follower profile is not found'}

    if what == 'AUTHOR':
        error = author_unfollow(follower.id, slug)
        if not error:
            logger.info(f'@{follower.slug} unfollowing @{slug}')
            [author] = await get_authors_with_stat_cached(
                select(Author).where(Author.slug == slug)
            )
            if not author:
                return {'error': 'cant find author'}
            _followers = await update_followers_for_author(follower, author, False)
            await notify_follower(follower.dict(), author.id, 'unfollow')
            follows = await update_follows_for_author(
                follower, 'author', author.dict(), False
            )

    elif what == 'TOPIC':
        error = topic_unfollow(follower.id, slug)
        if not error:
            logger.info(f'@{follower.slug} unfollowing §{slug}')
            [topic] = await get_topics_with_stat_cached(
                select(Topic).where(Topic.slug == slug)
            )
            if not topic:
                return {'error': 'cant find topic'}
            follows = await update_follows_for_author(
                follower, 'topic', topic.dict(), False
            )

    elif what == 'COMMUNITY':
        follows = local_session().execute(select(Community))

    elif what == 'SHOUT':
        error = reactions_unfollow(follower.id, slug)
        if not error:
            logger.info(f'@{follower.slug} unfollowing §{slug}')
            [shout] = local_session().execute(select(Shout).where(Shout.slug == slug))
            if not shout:
                return {'error': 'cant find shout'}
            if not error:
                follows = await update_follows_for_author(
                    follower, 'shout', shout.dict(), False
                )

    return {'error': error, f'{what.lower()}s': follows}


async def get_follows_by_user_id(user_id: str):
    if not user_id:
        return {'error': 'unauthorized'}
    author = await redis.execute('GET', f'user:{user_id}')
    if isinstance(author, str):
        author = json.loads(author)
    if not author:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            if not author:
                return {'error': 'cant find author'}
            author = author.dict()
    last_seen = author.get('last_seen', 0) if isinstance(author, dict) else 0
    follows = DEFAULT_FOLLOWS
    day_old = int(time.time()) - last_seen > 24 * 60 * 60
    if day_old:
        author_id = json.loads(str(author)).get('id')
        if author_id:
            topics = author_follows_topics(author_id)
            authors = author_follows_authors(author_id)
            follows = {
                'topics': topics,
                'authors': authors,
                'communities': [
                    {'id': 1, 'name': 'Дискурс', 'slug': 'discours', 'pic': ''}
                ],
            }
    else:
        logger.debug(f'getting follows for {user_id} from redis')
        res = await redis.execute('GET', f'user:{user_id}:follows')
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
        return 'already followed'
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
        return 'already unfollowed'
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
        return 'already followed'
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
        return 'already unfollowed'
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
        return 'already followed'
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
        return 'already unfollowed'
    except Exception as exc:
        return exc


@query.field('get_topic_followers')
async def get_topic_followers(_, _info, slug: str, topic_id: int) -> List[Author]:
    q = select(Author)
    q = (
        q.join(TopicFollower, TopicFollower.follower == Author.id)
        .join(Topic, Topic.id == TopicFollower.topic)
        .filter(or_(Topic.slug == slug, Topic.id == topic_id))
    )
    return await get_authors_with_stat_cached(q)


@query.field('get_shout_followers')
def get_shout_followers(
    _, _info, slug: str = '', shout_id: int | None = None
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
