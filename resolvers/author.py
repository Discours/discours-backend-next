import asyncio
import time

from sqlalchemy import desc, select, text

from cache.cache import (
    cache_author,
    get_cached_author,
    get_cached_author_by_user_id,
    get_cached_author_followers,
    get_cached_follower_authors,
    get_cached_follower_topics,
)
from orm.author import Author
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from utils.logger import root_logger as logger


@mutation.field("update_author")
@login_required
async def update_author(_, info, profile):
    user_id = info.context.get("user_id")
    if not user_id:
        return {"error": "unauthorized", "author": None}
    try:
        with local_session() as session:
            author = session.query(Author).where(Author.user == user_id).first()
            if author:
                Author.update(author, profile)
                session.add(author)
                session.commit()
                author_query = select(Author).where(Author.user == user_id)
                result = get_with_stat(author_query)
                if result:
                    author_with_stat = result[0]
                    if isinstance(author_with_stat, Author):
                        author_dict = author_with_stat.dict()
                        # await cache_author(author_dict)
                        asyncio.create_task(cache_author(author_dict))
                return {"error": None, "author": author}
    except Exception as exc:
        import traceback

        logger.error(traceback.format_exc())
        return {"error": exc, "author": None}


@query.field("get_authors_all")
def get_authors_all(_, _info):
    with local_session() as session:
        authors = session.query(Author).all()
        return authors


@query.field("get_author")
async def get_author(_, _info, slug="", author_id=0):
    author_dict = None
    try:
        author_id = get_author_id_from(slug=slug, user="", author_id=author_id)
        if not author_id:
            raise ValueError("cant find")
        author_dict = await get_cached_author(int(author_id), get_with_stat)

        if not author_dict or not author_dict.get("stat"):
            # update stat from db
            author_query = select(Author).filter(Author.id == author_id)
            result = get_with_stat(author_query)
            if result:
                author_with_stat = result[0]
                if isinstance(author_with_stat, Author):
                    author_dict = author_with_stat.dict()
                    # await cache_author(author_dict)
                    asyncio.create_task(cache_author(author_dict))
    except ValueError:
        pass
    except Exception as exc:
        import traceback

        logger.error(f"{exc}:\n{traceback.format_exc()}")
    return author_dict


@query.field("get_author_id")
async def get_author_id(_, _info, user: str):
    user_id = user.strip()
    logger.info(f"getting author id for {user_id}")
    author = None
    try:
        author = await get_cached_author_by_user_id(user_id, get_with_stat)
        if author:
            return author

        author_query = select(Author).filter(Author.user == user_id)
        result = get_with_stat(author_query)
        if result:
            author_with_stat = result[0]
            if isinstance(author_with_stat, Author):
                author_dict = author_with_stat.dict()
                # await cache_author(author_dict)
                asyncio.create_task(cache_author(author_dict))
                return author_with_stat
    except Exception as exc:
        import traceback

        traceback.print_exc()
        logger.error(exc)


@query.field("load_authors_by")
async def load_authors_by(_, _info, by, limit, offset):
    logger.debug(f"loading authors by {by}")
    authors_query = select(Author)

    if by.get("slug"):
        authors_query = authors_query.filter(Author.slug.ilike(f"%{by['slug']}%"))
    elif by.get("name"):
        authors_query = authors_query.filter(Author.name.ilike(f"%{by['name']}%"))
    elif by.get("topic"):
        authors_query = (
            authors_query.join(ShoutAuthor)  # Первое соединение ShoutAuthor
            .join(ShoutTopic, ShoutAuthor.shout == ShoutTopic.shout)
            .join(Topic, ShoutTopic.topic == Topic.id)
            .filter(Topic.slug == str(by["topic"]))
        )

    if by.get("last_seen"):  # в unix time
        before = int(time.time()) - by["last_seen"]
        authors_query = authors_query.filter(Author.last_seen > before)
    elif by.get("created_at"):  # в unix time
        before = int(time.time()) - by["created_at"]
        authors_query = authors_query.filter(Author.created_at > before)

    authors_query = authors_query.limit(limit).offset(offset)

    with local_session() as session:
        authors_nostat = session.execute(authors_query).all()
        authors = []
        for a in authors_nostat:
            if isinstance(a, Author):
                author_dict = await get_cached_author(a.id, get_with_stat)
                if author_dict and isinstance(author_dict.get("shouts"), int):
                    authors.append(author_dict)

    # order
    order = by.get("order")
    if order in ["shouts", "followers"]:
        authors_query = authors_query.order_by(desc(text(f"{order}_stat")))

    # group by
    authors = get_with_stat(authors_query)
    return authors or []


def get_author_id_from(slug="", user=None, author_id=None):
    if not slug and not user and not author_id:
        raise ValueError("One of slug, user, or author_id must be provided")

    author_query = select(Author.id)
    if user:
        author_query = author_query.filter(Author.user == user)
    elif slug:
        author_query = author_query.filter(Author.slug == slug)
    elif author_id:
        author_query = author_query.filter(Author.id == author_id)

    with local_session() as session:
        author_id_result = session.execute(author_query).first()
        author_id = author_id_result[0] if author_id_result else None

    if not author_id:
        raise ValueError("Author not found")

    return author_id


@query.field("get_author_follows")
async def get_author_follows(_, _info, slug="", user=None, author_id=0):
    try:
        author_id = get_author_id_from(slug, user, author_id)

        if bool(author_id):
            logger.debug(f"getting {author_id} follows authors")
            authors = await get_cached_follower_authors(author_id)
            topics = await get_cached_follower_topics(author_id)
        return {
            "topics": topics,
            "authors": authors,
            "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
        }
    except Exception:
        import traceback

        traceback.print_exc()
        return {"error": "Author not found"}


@query.field("get_author_follows_topics")
async def get_author_follows_topics(_, _info, slug="", user=None, author_id=None):
    try:
        follower_id = get_author_id_from(slug, user, author_id)
        topics = await get_cached_follower_topics(follower_id)
        return topics
    except Exception:
        import traceback

        traceback.print_exc()


@query.field("get_author_follows_authors")
async def get_author_follows_authors(_, _info, slug="", user=None, author_id=None):
    try:
        follower_id = get_author_id_from(slug, user, author_id)
        return await get_cached_follower_authors(follower_id)
    except Exception:
        import traceback

        traceback.print_exc()


def create_author(user_id: str, slug: str, name: str = ""):
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
                logger.info(f"author created by webhook {new_author.dict()}")
        except Exception as exc:
            logger.debug(exc)


@query.field("get_author_followers")
async def get_author_followers(_, _info, slug: str = "", user: str = "", author_id: int = 0):
    logger.debug(f"getting followers for @{slug}")
    author_id = get_author_id_from(slug=slug, user=user, author_id=author_id)
    followers = []
    if author_id:
        followers = await get_cached_author_followers(author_id)
    return followers
