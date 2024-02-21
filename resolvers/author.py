import json
import time
from typing import List

from sqlalchemy import and_, desc, distinct, func, select, or_
from sqlalchemy.orm import aliased

from orm.author import Author, AuthorFollower, AuthorRating
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.follower import get_follows_by_user_id
from services.auth import login_required
from services.db import local_session
from services.rediscache import redis
from services.schema import mutation, query
from services.viewed import ViewedStorage
from services.logger import root_logger as logger


def add_author_stat_columns(q):
    shout_author_aliased = aliased(ShoutAuthor)
    q = q.outerjoin(shout_author_aliased).add_columns(
        func.count(distinct(shout_author_aliased.shout)).label('shouts_stat')
    )

    followers_table = aliased(AuthorFollower)
    q = q.outerjoin(followers_table, followers_table.author == Author.id).add_columns(
        func.count(distinct(followers_table.follower)).label('followers_stat')
    )

    followings_table = aliased(AuthorFollower)
    q = q.outerjoin(
        followings_table, followings_table.follower == Author.id
    ).add_columns(func.count(distinct(followers_table.author)).label('followings_stat'))

    q = q.group_by(Author.id)
    return q


async def get_authors_from_query(q):
    authors = []
    with local_session() as session:
        for [author, shouts_stat, followers_stat, followings_stat] in session.execute(
            q
        ):
            author.stat = {
                'shouts': shouts_stat,
                'viewed': await ViewedStorage.get_author(author.slug),
                'followers': followers_stat,
                'followings': followings_stat,
            }
            authors.append(author)
    return authors


@mutation.field('update_author')
@login_required
async def update_author(_, info, profile):
    user_id = info.context['user_id']
    with local_session() as session:
        author = session.query(Author).where(Author.user == user_id).first()
        Author.update(author, profile)
        session.add(author)
        session.commit()
    return {'error': None, 'author': author}


# TODO: caching query
@query.field('get_authors_all')
async def get_authors_all(_, _info):
    authors = []
    with local_session() as session:
        authors = session.query(Author).all()
    return authors


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


async def load_author_with_stats(q):
    q = add_author_stat_columns(q)

    result = await get_authors_from_query(q)

    if result:
        [author] = result
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
            likes_count = (
                session.query(AuthorRating)
                .filter(
                    and_(AuthorRating.author == author.id, AuthorRating.plus.is_(True))
                )
                .count()
            )
            dislikes_count = (
                session.query(AuthorRating)
                .filter(
                    and_(
                        AuthorRating.author == author.id, AuthorRating.plus.is_not(True)
                    )
                )
                .count()
            )
            author.stat['rating'] = likes_count - dislikes_count
            author.stat['rating_shouts'] = count_author_shouts_rating(
                session, author.id
            )
            author.stat['rating_comments'] = count_author_comments_rating(
                session, author.id
            )
            author.stat['commented'] = comments_count
            return author


@query.field('get_author')
async def get_author(_, _info, slug='', author_id=None):
    q = None
    if slug or author_id:
        if bool(slug):
            q = select(Author).where(Author.slug == slug)
        if author_id:
            q = select(Author).where(Author.id == author_id)

        return await load_author_with_stats(q)


async def get_author_by_user_id(user_id: str):
    redis_key = f'user:{user_id}:author'
    res = await redis.execute('GET', redis_key)
    if isinstance(res, str):
        author = json.loads(res)
        if author.get('id'):
            logger.debug(f'got cached author: {author}')
            return author

    logger.info(f'getting author id for {user_id}')
    q = select(Author).filter(Author.user == user_id)
    author = await load_author_with_stats(q)
    if author:
        await redis.execute(
            'set',
            redis_key,
            json.dumps(
                {
                    'id': author.id,
                    'name': author.name,
                    'slug': author.slug,
                    'pic': author.pic,
                }
            ),
        )
        return author


@query.field('get_author_id')
async def get_author_id(_, _info, user: str):
    return await get_author_by_user_id(user)


@query.field('load_authors_by')
async def load_authors_by(_, _info, by, limit, offset):
    q = select(Author)
    q = add_author_stat_columns(q)
    if by.get('slug'):
        q = q.filter(Author.slug.ilike(f"%{by['slug']}%"))
    elif by.get('name'):
        q = q.filter(Author.name.ilike(f"%{by['name']}%"))
    elif by.get('topic'):
        q = (
            q.join(ShoutAuthor)
            .join(ShoutTopic)
            .join(Topic)
            .where(Topic.slug == by['topic'])
        )

    if by.get('last_seen'):  # in unix time
        before = int(time.time()) - by['last_seen']
        q = q.filter(Author.last_seen > before)
    elif by.get('created_at'):  # in unix time
        before = int(time.time()) - by['created_at']
        q = q.filter(Author.created_at > before)

    order = by.get('order')
    if order == 'followers' or order == 'shouts':
        q = q.order_by(desc(f'{order}_stat'))

    q = q.limit(limit).offset(offset)

    authors = await get_authors_from_query(q)

    return authors


@query.field('get_author_follows')
async def get_author_follows(
    _, _info, slug='', user=None, author_id=None
) -> List[Author]:
    user_id = user
    if not user_id and author_id or slug:
        with local_session() as session:
            author = (
                session.query(Author)
                .where(or_(Author.id == author_id, Author.slug == slug))
                .first()
            )
            user_id = author.user

    if user_id:
        follows = await get_follows_by_user_id(user_id)
        return follows

    else:
        raise ValueError('Author not found')


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


async def create_author(user_id: str, slug: str, name: str = ''):
    with local_session() as session:
        new_author = Author(user=user_id, slug=slug, name=name)
        session.add(new_author)
        session.commit()
        logger.info(f'author created by webhook {new_author.dict()}')


@query.field('get_author_followers')
async def get_author_followers(_, _info, slug) -> List[Author]:
    q = select(Author)
    q = add_author_stat_columns(q)

    aliased_author = aliased(Author)
    q = (
        q.join(AuthorFollower, AuthorFollower.follower == Author.id)
        .join(aliased_author, aliased_author.id == AuthorFollower.author)
        .where(aliased_author.slug == slug)
    )

    return await get_authors_from_query(q)
