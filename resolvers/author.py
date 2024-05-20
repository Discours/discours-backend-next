import time

from sqlalchemy import desc, or_, select, text

from orm.author import Author
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.cache import (
    cache_author,
    get_cached_author,
    get_cached_author_by_user_id,
    get_cached_author_followers,
    get_cached_author_follows_authors,
    get_cached_author_follows_topics,
)
from services.db import local_session
from services.logger import root_logger as logger
from services.schema import mutation, query


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
                [author] = get_with_stat(author_query)
                if author:
                    await cache_author(author.dict())
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
    author_query = ""
    author = None
    author_dict = None
    try:
        # lookup for cached author
        author_query = select(Author)
        if slug:
            author_query = author_query.filter(Author.slug == slug)
        elif author_id:
            author_query = author_query.filter(Author.id == author_id)
        else:
            raise ValueError("Author not found")
        lookup_result = local_session().execute(author_query).first()
        if lookup_result:
            [found_author] = lookup_result
            # logger.debug(found_author)
            if found_author:
                logger.debug(f"found author id: {found_author.id}")
                author_id = found_author.id if found_author.id else author_id
                author_dict = await get_cached_author(author_id)

        # update stat from db
        if not author_dict or not author_dict.get("stat"):
            result = get_with_stat(author_query)
            if not result:
                raise ValueError("Author not found")
            [author] = result
            # use found author
            if isinstance(author, Author):
                logger.debug(f"update @{author.slug} with id {author.id}")
                author_dict = author.dict()
                await cache_author(author_dict)
    except ValueError:
        pass
    except Exception as exc:
        import traceback

        logger.error(f"{exc}:\n{traceback.format_exc()}")
    return author_dict


async def get_author_by_user_id(user_id: str):
    logger.info(f"getting author id for {user_id}")
    author = None
    try:
        author = await get_cached_author_by_user_id(user_id)
        if author:
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


@query.field("get_author_id")
async def get_author_id(_, _info, user: str):
    return await get_author_by_user_id(user)


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
            authors_query.join(ShoutAuthor)
            .join(ShoutTopic)
            .join(Topic)
            .where(Topic.slug == str(by["topic"]))
        )

    if by.get("last_seen"):  # in unix time
        before = int(time.time()) - by["last_seen"]
        authors_query = authors_query.filter(Author.last_seen > before)
    elif by.get("created_at"):  # in unix time
        before = int(time.time()) - by["created_at"]
        authors_query = authors_query.filter(Author.created_at > before)
    authors_query = authors_query.limit(limit).offset(offset)
    authors_nostat = local_session().execute(authors_query)
    authors = []
    if authors_nostat:
        for [a] in authors_nostat:
            author_dict = None
            if isinstance(a, Author):
                author_dict = await get_cached_author(a.id)
                if not author_dict or not isinstance(author_dict.get("shouts"), int):
                    break

    # order
    order = by.get("order")
    if order in ["shouts", "followers"]:
        authors_query = authors_query.order_by(desc(text(f"{order}_stat")))
    # group by
    authors = get_with_stat(authors_query)
    return authors or []


@query.field("get_author_follows")
async def get_author_follows(_, _info, slug="", user=None, author_id=0):
    try:
        author_query = select(Author)
        if user:
            author_query = author_query.filter(Author.user == user)
        elif slug:
            author_query = author_query.filter(Author.slug == slug)
        elif author_id:
            author_query = author_query.filter(Author.id == author_id)
        else:
            return {"error": "One of slug, user, or author_id must be provided"}
        result = local_session().execute(author_query)
        if result:
            # logger.debug(result)
            [author] = result
            # logger.debug(author)
            if author and isinstance(author, Author):
                # logger.debug(author.dict())
                author_id = author.id if not author_id else author_id
                topics = []
                authors = []
                if bool(author_id):
                    logger.debug(f"getting {author_id} follows authors")
                    authors = await get_cached_author_follows_authors(author_id)
                    topics = await get_cached_author_follows_topics(author_id)
                return {
                    "topics": topics,
                    "authors": authors,
                    "communities": [
                        {"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}
                    ],
                }
    except Exception:
        import traceback

        traceback.print_exc()
    return {"error": "Author not found"}


@query.field("get_author_follows_topics")
async def get_author_follows_topics(_, _info, slug="", user=None, author_id=None):
    with local_session() as session:
        if user or slug:
            author_id_result = (
                session.query(Author.id)
                .filter(or_(Author.user == user, Author.slug == slug))
                .first()
            )
            author_id = author_id_result[0] if author_id_result else None
        if not author_id:
            raise ValueError("Author not found")
        return get_author_follows_topics(author_id)


@query.field("get_author_follows_authors")
async def get_author_follows_authors(_, _info, slug="", user=None, author_id=None):
    with local_session() as session:
        if user or slug:
            author_id_result = (
                session.query(Author.id)
                .filter(or_(Author.user == user, Author.slug == slug))
                .first()
            )
            author_id = author_id_result[0] if author_id_result else None
        if not author_id:
            raise ValueError("Author not found")
        return await get_cached_author_follows_authors(author_id)


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
async def get_author_followers(_, _info, slug: str):
    logger.debug(f"getting followers for @{slug}")
    author_query = select(Author.id).filter(Author.slug == slug).first()
    author_id_result = local_session().execute(author_query)
    author_id = author_id_result[0] if author_id_result else None
    followers = await get_cached_author_followers(author_id)
    return followers
