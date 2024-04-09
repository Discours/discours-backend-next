import json
import time

from sqlalchemy import and_, desc, or_, select, text
from sqlalchemy.orm import aliased
from sqlalchemy_searchable import search

from orm.author import Author, AuthorFollower
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.stat import author_follows_authors, author_follows_topics, get_with_stat
from services.auth import login_required
from services.cache import cache_author, cache_follower
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.memorycache import cache_region
from services.rediscache import redis
from services.schema import mutation, query


@mutation.field('update_author')
@login_required
async def update_author(_, info, profile):
    user_id = info.context.get('user_id')
    if not user_id:
        return {'error': 'unauthorized', 'author': None}
    try:
        with local_session() as session:
            author = session.query(Author).where(Author.user == user_id).first()
            if author:
                Author.update(author, profile)
                session.add(author)
                session.commit()
                return {'error': None, 'author': author}
    except Exception as exc:
        import traceback

        logger.error(traceback.format_exc())
        return {'error': exc, 'author': None}


@query.field('get_authors_all')
def get_authors_all(_, _info):
    with local_session() as session:
        authors = session.query(Author).all()
        return authors


@query.field('get_author')
async def get_author(_, _info, slug='', author_id=0):
    author_query = ''
    author = None
    author_dict = None
    try:
        # lookup for cached author
        author_query = select(Author).filter(or_(Author.slug == slug, Author.id == author_id))
        [found_author] = local_session().execute(author_query).first()
        logger.debug(found_author)
        if found_author:
            logger.debug(f'found author id: {found_author.id}')
            author_id = found_author.id if found_author.id else author_id
            if author_id:
                cached_result = await redis.execute('GET', f'author:{author_id}')
                author_dict = json.loads(cached_result) if cached_result else None

        # update stat from db
        if not author_dict or not author_dict.get('stat'):
            result = get_with_stat(author_query)
            if not result:
                raise ValueError('Author not found')
            [author] = result
            # use found author
            if isinstance(author, Author):
                logger.debug(f'update @{author.slug} with id {author.id}')
                author_dict = author.dict()
                await cache_author(author_dict)
    except ValueError:
        pass
    except Exception as exc:
        import traceback
        logger.error(f'{exc}:\n{traceback.format_exc()}')
    return author_dict


async def get_author_by_user_id(user_id: str):
    logger.info(f'getting author id for {user_id}')
    redis_key = f'user:{user_id}'
    author = None
    try:
        res = await redis.execute('GET', redis_key)
        if isinstance(res, str):
            author = json.loads(res)
            author_id = author.get('id')
            author_slug = author.get('slug')
            if author_id:
                logger.debug(f'got author @{author_slug} #{author_id} cached')
                return author

        author_query = select(Author).filter(Author.user == user_id)
        result = get_with_stat(author_query)
        if result:
            [author] = result
            await cache_author(author.dict())
    except Exception as exc:
        import traceback

        traceback.print_exc()
        logger.error(exc)
    return author


@query.field('get_author_id')
async def get_author_id(_, _info, user: str):
    return await get_author_by_user_id(user)


@query.field('load_authors_by')
def load_authors_by(_, _info, by, limit, offset):
    cache_key = f'{json.dumps(by)}_{limit}_{offset}'

    @cache_region.cache_on_arguments(cache_key)
    def _load_authors_by():
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

        # q = q.distinct()
        q = q.limit(limit).offset(offset)

        authors = get_with_stat(q)

        return authors

    return _load_authors_by()


@query.field('get_author_follows')
async def get_author_follows(_, _info, slug='', user=None, author_id=0):
    try:
        author_query = select(Author)
        if user:
            author_query = author_query.filter(Author.user == user)
        elif slug:
            author_query = author_query.filter(Author.slug == slug)
        elif author_id:
            author_query = author_query.filter(Author.id == author_id)
        else:
            raise ValueError('One of slug, user, or author_id must be provided')
        [result] = local_session().execute(author_query)
        if len(result) > 0:
            # logger.debug(result)
            [author] = result
            # logger.debug(author)
            if author and isinstance(author, Author):
                # logger.debug(author.dict())
                author_id = author.id
                rkey = f'author:{author_id}:follows-authors'
                logger.debug(f'getting {author_id} follows authors')
                cached = await redis.execute('GET', rkey)
                authors = []
                if not cached:
                    authors = author_follows_authors(author_id)
                    prepared = [author.dict() for author in authors]
                    await redis.execute('SET', rkey, json.dumps(prepared, cls=CustomJSONEncoder))
                elif isinstance(cached, str):
                    authors = json.loads(cached)

                rkey = f'author:{author_id}:follows-topics'
                cached = await redis.execute('GET', rkey)
                topics = []
                if cached and isinstance(cached, str):
                    topics = json.loads(cached)
                if not cached:
                    topics = author_follows_topics(author_id)
                    prepared = [topic.dict() for topic in topics]
                    await redis.execute(
                        'SET', rkey, json.dumps(prepared, cls=CustomJSONEncoder)
                    )
                return {
                    'topics': topics,
                    'authors': authors,
                    'communities': [
                        {'id': 1, 'name': 'Дискурс', 'slug': 'discours', 'pic': ''}
                    ],
                }
    except Exception:
        import traceback

        traceback.print_exc()
    return {'error': 'Author not found'}


@query.field('get_author_follows_topics')
async def get_author_follows_topics(_, _info, slug='', user=None, author_id=None):
    with local_session() as session:
        if user or slug:
            author_id_result = (
                session.query(Author.id)
                .filter(or_(Author.user == user, Author.slug == slug))
                .first()
            )
            author_id = author_id_result[0] if author_id_result else None
        if not author_id:
            raise ValueError('Author not found')
        logger.debug(f'getting {author_id} follows topics')
        rkey = f'author:{author_id}:follows-topics'
        cached = await redis.execute('GET', rkey)
        topics = []
        if isinstance(cached, str):
            topics = json.loads(cached)
        if not cached:
            topics = author_follows_topics(author_id)
            prepared = [topic.dict() for topic in topics]
            await redis.execute(
                'SET', rkey, json.dumps(prepared, cls=CustomJSONEncoder)
            )
        return topics


@query.field('get_author_follows_authors')
async def get_author_follows_authors(_, _info, slug='', user=None, author_id=None):
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
            rkey = f'author:{author_id}:follows-authors'
            cached = await redis.execute('GET', rkey)
            authors = []
            if isinstance(cached, str):
                authors = json.loads(cached)
            if not authors:
                authors = author_follows_authors(author_id)
                prepared = [author.dict() for author in authors]
                await redis.execute(
                    'SET', rkey, json.dumps(prepared, cls=CustomJSONEncoder)
                )
            return authors
        else:
            raise ValueError('Author not found')


def create_author(user_id: str, slug: str, name: str = ''):
    with local_session() as session:
        try:
            author = None
            if user_id:
                author = session.query(Author).filter(Author.user == user_id).first()
            elif slug:
                author = session.query(Author).filter(Author.slug == slug).first()
            if not author:
                new_author = Author(user=user_id, slug=slug, name=name)
                session.add(new_author)
                session.commit()
                logger.info(f'author created by webhook {new_author.dict()}')
        except Exception as exc:
            logger.debug(exc)


@query.field('get_author_followers')
async def get_author_followers(_, _info, slug: str):
    logger.debug(f'getting followers for @{slug}')
    try:
        author_alias = aliased(Author)
        author_query = select(author_alias).filter(author_alias.slug == slug)
        result = local_session().execute(author_query).first()
        if result:
            [author] = result
            author_id = author.id
            cached = await redis.execute('GET', f'author:{author_id}:followers')
            if not cached:
                author_follower_alias = aliased(AuthorFollower, name='af')
                q = select(Author).join(
                    author_follower_alias,
                    and_(
                        author_follower_alias.author == author_id,
                        author_follower_alias.follower == Author.id,
                    ),
                )
                results = get_with_stat(q)
                if isinstance(results, list):
                    for follower in results:
                        await cache_follower(follower, author)
                    logger.debug(f'@{slug} cache updated with {len(results)} followers')
                return results
            else:
                logger.debug(f'@{slug} got followers cached')
                if isinstance(cached, str):
                    return json.loads(cached)
    except Exception as exc:
        import traceback

        logger.error(exc)
        logger.error(traceback.format_exc())
        return []


@query.field('search_authors')
async def search_authors(_, _info, what: str):
    q = search(select(Author), what)
    return get_with_stat(q)
