import json
import time
from typing import List

from sqlalchemy import select, or_
from sqlalchemy.sql import and_

from orm.author import Author, AuthorFollower

# from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.community import community_follow, community_unfollow
from resolvers.topic import topic_unfollow, topic_follow
from resolvers.stat import get_with_stat, author_follows_topics, author_follows_authors
from services.auth import login_required
from services.db import local_session
from services.cache import (
    DEFAULT_FOLLOWS,
    update_follows_for_author,
    update_followers_for_author,
)
from services.notify import notify_follower
from services.schema import mutation, query
from services.logger import root_logger as logger
from services.rediscache import redis


@mutation.field('follow')
@login_required
async def follow(_, info, what, slug):
    follows = []
    try:
        user_id = info.context['user_id']
        follower_query = (
            select(Author).select_from(Author).filter(Author.user == user_id)
        )
        [follower] = get_with_stat(follower_query)
        if follower:
            if what == 'AUTHOR':
                if author_follow(follower.id, slug):
                    logger.debug(f'@{follower.slug} followed @{slug}')
                    [author] = get_with_stat(select(Author).select_from(Author).where(Author.slug == slug))
                    if author:
                        follows = await update_follows_for_author(
                            follower, 'author', author, True
                        )
                        _followers = await update_followers_for_author(follower, author, True)
                        await notify_follower(follower.dict(), author.id, 'unfollow')
            elif what == 'TOPIC':
                topic_query = select(Topic).where(Topic.slug == slug)
                [topic] = get_with_stat(topic_query)
                if topic:
                    follows = await update_follows_for_author(
                        follower, 'topic', topic, True
                    )
                topic_follow(follower.id, slug)
            elif what == 'COMMUNITY':
                community_follow(follower.id, slug)
            elif what == 'REACTIONS':
                reactions_follow(follower.id, slug)

        return {f'{what.lower()}s': follows}
    except Exception as e:
        logger.error(e)
        return {'error': str(e)}


@mutation.field('unfollow')
@login_required
async def unfollow(_, info, what, slug):
    follows = None
    try:
        user_id = info.context.get('user_id')
        if not user_id:
            return {"error": "unauthorized"}
        follower_query = select(Author).filter(Author.user == user_id)
        [follower] = get_with_stat(follower_query)
        if follower:
            if what == 'AUTHOR':
                logger.info(f'@{follower.slug} unfollowing @{slug}')
                if author_unfollow(follower.id, slug):
                    author_query = select(Author).where(Author.slug == slug)
                    [author] = get_with_stat(author_query)
                    if author:
                        follows = await update_follows_for_author(
                            follower, 'author', author, False
                        )
                        _followers = await update_followers_for_author(
                            follower, author, False
                        )
                        await notify_follower(follower.dict(), author.id, 'unfollow')
            elif what == 'TOPIC':
                logger.info(f'@{follower.slug} unfollowing §{slug}')
                topic_query = select(Topic).where(Topic.slug == slug)
                [topic] = get_with_stat(topic_query)
                if topic:
                    follows = await update_follows_for_author(
                        follower, 'topic', topic, False
                    )
                topic_unfollow(follower.id, slug)
            elif what == 'COMMUNITY':
                community_unfollow(follower.id, slug)
            elif what == 'REACTIONS':
                reactions_unfollow(follower.id, slug)
        return {'error': "", f'{what.lower()}s': follows}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'error': str(e)}


async def get_follows_by_user_id(user_id: str):
    if not user_id:
        return {"error": "unauthorized"}
    author = await redis.execute('GET', f'user:{user_id}:author')
    if isinstance(author, str):
        author = json.loads(author)
    if not author:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            if not author:
                return {"error": "cant find author"}
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
        import traceback

        traceback.print_exc()
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
    except Exception as exc:
        logger.error(exc)
        import traceback

        traceback.print_exc()
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


@query.field('get_topic_followers')
def get_topic_followers(_, _info, slug: str, topic_id: int) -> List[Author]:
    q = select(Author)
    q = (
        q.join(TopicFollower, TopicFollower.follower == Author.id)
        .join(Topic, Topic.id == TopicFollower.topic)
        .filter(or_(Topic.slug == slug, Topic.id == topic_id))
    )
    return get_with_stat(q)


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
