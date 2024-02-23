import json
import time
from typing import List

from sqlalchemy import select, or_, func
from sqlalchemy.orm import aliased
from sqlalchemy.sql import and_

from orm.author import Author, AuthorFollower

# from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.community import community_follow, community_unfollow
from resolvers.topic import topic_follow, topic_unfollow
from resolvers.stat import get_authors_with_stat
from services.auth import login_required
from services.db import local_session
from services.follows import DEFAULT_FOLLOWS
from services.notify import notify_follower
from services.schema import mutation, query
from services.logger import root_logger as logger
from services.rediscache import redis


@mutation.field('follow')
@login_required
async def follow(_, info, what, slug):
    try:
        user_id = info.context['user_id']
        with local_session() as session:
            actor = session.query(Author).filter(Author.user == user_id).first()
            if actor:
                follower_id = actor.id
                if what == 'AUTHOR':
                    if author_follow(follower_id, slug):
                        author = (
                            session.query(Author.id).where(Author.slug == slug).one()
                        )
                        follower = (
                            session.query(Author).where(Author.id == follower_id).one()
                        )
                        await notify_follower(follower.dict(), author.id)
                elif what == 'TOPIC':
                    topic_follow(follower_id, slug)
                elif what == 'COMMUNITY':
                    community_follow(follower_id, slug)
                elif what == 'REACTIONS':
                    reactions_follow(follower_id, slug)
    except Exception as e:
        logger.debug(info, what, slug)
        logger.error(e)
        return {'error': str(e)}

    return {}


@mutation.field('unfollow')
@login_required
async def unfollow(_, info, what, slug):
    user_id = info.context['user_id']
    try:
        with local_session() as session:
            actor = session.query(Author).filter(Author.user == user_id).first()
            if actor:
                follower_id = actor.id
                if what == 'AUTHOR':
                    if author_unfollow(follower_id, slug):
                        author = (
                            session.query(Author.id).where(Author.slug == slug).one()
                        )
                        follower = (
                            session.query(Author).where(Author.id == follower_id).one()
                        )
                        await notify_follower(follower.dict(), author.id, 'unfollow')
                elif what == 'TOPIC':
                    topic_unfollow(follower_id, slug)
                elif what == 'COMMUNITY':
                    community_unfollow(follower_id, slug)
                elif what == 'REACTIONS':
                    reactions_unfollow(follower_id, slug)
    except Exception as e:
        return {'error': str(e)}

    return {}


def query_follows(user_id: str):
    logger.debug(f'query follows for {user_id} from database')
    topics = []
    authors = []
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if isinstance(author, Author):
            author_id = author.id
            aliased_author = aliased(Author)
            aliased_author_followers = aliased(AuthorFollower)
            aliased_author_authors = aliased(AuthorFollower)

            authors = (
                session.query(
                    aliased_author,
                    func.count(func.distinct(ShoutAuthor.shout)).label("shouts_stat"),
                    func.count(func.distinct(AuthorFollower.author)).label("authors_stat"),
                    func.count(func.distinct(AuthorFollower.follower)).label("followers_stat")
                )
                .filter(AuthorFollower.author == aliased_author.id)
                .join(AuthorFollower, AuthorFollower.follower == aliased_author.id)
                .outerjoin(ShoutAuthor, ShoutAuthor.author == author_id)
                .outerjoin(aliased_author_authors, AuthorFollower.follower == author_id)
                .outerjoin(aliased_author_followers, AuthorFollower.author == author_id)
                .group_by(aliased_author.id)
                .all()
            )

            aliased_shout_authors = aliased(ShoutAuthor)
            aliased_topic_followers = aliased(TopicFollower)
            aliased_topic = aliased(Topic)
            topics = (
                session.query(
                    aliased_topic,
                    func.count(func.distinct(ShoutTopic.shout)).label("shouts_stat"),
                    func.count(func.distinct(ShoutAuthor.author)).label("authors_stat"),
                    func.count(func.distinct(TopicFollower.follower)).label("followers_stat")
                )
                .join(TopicFollower, TopicFollower.topic == aliased_topic.id)
                .outerjoin(ShoutTopic, aliased_topic.id == ShoutTopic.topic)
                .outerjoin(aliased_shout_authors, ShoutTopic.shout == aliased_shout_authors.shout)
                .outerjoin(aliased_topic_followers, aliased_topic_followers.topic == aliased_topic.id)
                .group_by(aliased_topic.id)
                .all()
            )

    return {
        'topics': topics,
        'authors': authors,
        'communities': [{'id': 1, 'name': 'Дискурс', 'slug': 'discours'}],
    }


async def get_follows_by_user_id(user_id: str):
    if user_id:
        author = await redis.execute('GET', f'user:{user_id}:author')
        follows = DEFAULT_FOLLOWS
        day_old = int(time.time()) - author.get('last_seen', 0) > 24*60*60
        if day_old:
            follows = query_follows(user_id)
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


@query.field('get_topic_followers')
def get_topic_followers(_, _info, slug: str, topic_id: int) -> List[Author]:
    q = select(Author)
    q = (
        q.join(TopicFollower, TopicFollower.follower == Author.id)
        .join(Topic, Topic.id == TopicFollower.topic)
        .filter(or_(Topic.slug == slug, Topic.id == topic_id))
    )
    return get_authors_with_stat(q)


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
