import json
import time

from sqlalchemy import select, or_, and_, text, desc
from sqlalchemy.orm import aliased
from sqlalchemy_searchable import search

from orm.author import Author, AuthorFollower
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.stat import get_with_stat, author_follows_authors, author_follows_topics
from services.event_listeners import update_author_cache
from services.auth import login_required
from services.db import local_session
from services.rediscache import redis
from services.schema import mutation, query
from services.logger import root_logger as logger


@mutation.field('update_author')
@login_required
def update_author(_, info, profile):
    user_id = info.context['user_id']
    with local_session() as session:
        author = session.query(Author).where(Author.user == user_id).first()
        Author.update(author, profile)
        session.add(author)
        session.commit()
    return {'error': None, 'author': author}


@query.field('get_authors_all')
def get_authors_all(_, _info):
    with local_session() as session:
        authors = session.query(Author).all()
        return authors


@query.field('get_author')
async def get_author(_, _info, slug='', author_id=None):
    q = None
    author = None
    cache = None
    try:
        if slug:
            with local_session() as session:
                aliased_author = aliased(Author)
                q = select(aliased_author).filter(Author.slug == slug)
                [author] = session.execute(q)
                author_id = aliased_author.id
        if author_id:
            cache = await redis.execute('GET', f'id:{author_id}:author')
        if not cache:
            q = select(Author).where(Author.id == author_id)
            [author] = get_with_stat(q)
            if author:
                await update_author_cache(author)
        else:
            author = json.loads(cache)
    except Exception as exc:
        logger.error(exc)
    return author


async def get_author_by_user_id(user_id: str):
    logger.info(f'getting author id for {user_id}')
    redis_key = f'user:{user_id}:author'
    author = None
    try:
        res = await redis.execute('GET', redis_key)
        if isinstance(res, str):
            author = json.loads(res)
            if author.get('id'):
                logger.debug(f'got cached author: {author}')
                return author

        q = select(Author).filter(Author.user == user_id)

        [author] = get_with_stat(q)
        if author:
            await update_author_cache(author)
    except Exception as exc:
        logger.error(exc)
    return author


@query.field('get_author_id')
async def get_author_id(_, _info, user: str):
    return await get_author_by_user_id(user)


@query.field('load_authors_by')
def load_authors_by(_, _info, by, limit, offset):
    logger.debug(f'loading authors by {by}')
    q = select(Author)
    if by.get('slug'):
        q = q.filter(Author.slug.ilike(f"%{by['slug']}%"))
    elif by.get('name'):
        q = q.filter(Author.name.ilike(f"%{by['name']}%"))
    elif by.get('topic'):
        q = (
            q.join(ShoutAuthor)
            .join(ShoutTopic)
            .join(Topic)
            .where(Topic.slug == str(by['topic']))
        )

    if by.get('last_seen'):  # in unix time
        before = int(time.time()) - by['last_seen']
        q = q.filter(Author.last_seen > before)
    elif by.get('created_at'):  # in unix time
        before = int(time.time()) - by['created_at']
        q = q.filter(Author.created_at > before)

    order = by.get('order')
    if order in ['likes', 'shouts', 'followers']:
        q = q.order_by(desc(text(f'{order}_stat')))

    q = q.limit(limit).offset(offset)

    authors = get_with_stat(q)

    return authors


@query.field('get_author_follows')
def get_author_follows(_, _info, slug='', user=None, author_id=None):
    with local_session() as session:
        if user or slug:
            author_id_result = (
                session.query(Author.id)
                .filter(or_(Author.user == user, Author.slug == slug))
                .first()
            )
            author_id = author_id_result[0] if author_id_result else None
        if author_id:
            logger.debug(f'getting {author_id} follows')
            topics = author_follows_topics(author_id)
            authors = author_follows_authors(author_id)
            return {
                'topics': topics,
                'authors': authors,
                'communities': [
                    {'id': 1, 'name': 'Дискурс', 'slug': 'discours', 'pic': ''}
                ],
            }
        else:
            raise ValueError('Author not found')


@query.field('get_author_follows_topics')
def get_author_follows_topics(_, _info, slug='', user=None, author_id=None):
    with local_session() as session:
        if user or slug:
            author_id_result = (
                session.query(Author.id)
                .filter(or_(Author.user == user, Author.slug == slug))
                .first()
            )
            author_id = author_id_result[0] if author_id_result else None
        if author_id:
            logger.debug(f'getting {author_id} follows topics')
            follows = author_follows_topics(author_id)
            return follows
        else:
            raise ValueError('Author not found')


@query.field('get_author_follows_authors')
def get_author_follows_authors(_, _info, slug='', user=None, author_id=None):
    with local_session() as session:
        if user or slug:
            author_id_result = (
                session.query(Author.id)
                .filter(or_(Author.user == user, Author.slug == slug))
                .first()
            )
            author_id = author_id_result[0] if author_id_result else None
        if author_id:
            logger.debug(f'getting {author_id} follows authors')
            follows = author_follows_authors(author_id)
            return follows
        else:
            raise ValueError('Author not found')


def create_author(user_id: str, slug: str, name: str = ''):
    with local_session() as session:
        new_author = Author(user=user_id, slug=slug, name=name)
        session.add(new_author)
        session.commit()
        logger.info(f'author created by webhook {new_author.dict()}')


@query.field('get_author_followers')
def get_author_followers(_, _info, slug: str):
    logger.debug(f'getting followers for @{slug}')
    try:
        with local_session() as session:
            author_alias = aliased(Author)
            author_id_result = (
                session.query(author_alias.id).filter(author_alias.slug == slug).first()
            )
            author_id = author_id_result[0] if author_id_result else None

            author_follower_alias = aliased(AuthorFollower, name='af')
            q = select(Author).join(
                author_follower_alias,
                and_(
                    author_follower_alias.author == author_id,
                    author_follower_alias.follower == Author.id,
                ),
            )

            return get_with_stat(q)
    except Exception as exc:
        logger.error(exc)
        return []


@query.field('search_authors')
def search_authors(_, info, text: str):
    q = search(select(Author), text)
    return get_with_stat(q)
