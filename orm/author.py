import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy import event, select

from services.rediscache import redis
from orm.topic import Topic, TopicFollower
from services.db import Base


class AuthorRating(Base):
    __tablename__ = "author_rating"

    id = None  # type: ignore
    rater = Column(ForeignKey("author.id"), primary_key=True)
    author = Column(ForeignKey("author.id"), primary_key=True)
    plus = Column(Boolean)


class AuthorFollower(Base):
    __tablename__ = "author_follower"

    id = None  # type: ignore
    follower = Column(ForeignKey("author.id"), primary_key=True)
    author = Column(ForeignKey("author.id"), primary_key=True)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    auto = Column(Boolean, nullable=False, default=False)


class Author(Base):
    __tablename__ = "author"

    user = Column(String)  # unbounded link with authorizer's User type

    name = Column(String, nullable=True, comment="Display name")
    slug = Column(String, unique=True, comment="Author's slug")
    bio = Column(String, nullable=True, comment="Bio")  # status description
    about = Column(String, nullable=True, comment="About")  # long and formatted
    pic = Column(String, nullable=True, comment="Picture")
    links = Column(JSON, nullable=True, comment="Links")
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    last_seen = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    deleted_at = Column(Integer, nullable=True, comment="Deleted at")


@event.listens_for(Author, "after_insert")
@event.listens_for(Author, "after_update")
async def after_author_update(mapper, connection, target):
    redis_key = f"user:{target.user}:author"
    await redis.execute("HSET", redis_key, vars(target))


async def update_follows_for_user(connection, user_id, entity_type, entity, is_insert):
    redis_key = f"user:{user_id}:follows"
    follows = await redis.execute("HGET", redis_key)
    if not follows:
        follows = {
            "topics": [],
            "authors": [],
            "communities": [
                {"slug": "discours", "name": "Дискурс", "id": 1, "desc": ""}
            ],
        }
    if is_insert:
        follows[f"{entity_type}s"].append(entity)
    else:
        # Remove the entity from follows
        follows[f"{entity_type}s"] = [
            e for e in follows[f"{entity_type}s"] if e["id"] != entity.id
        ]

    await redis.execute("HSET", redis_key, vars(follows))


async def handle_author_follower_change(connection, author_id, follower_id, is_insert):
    author = connection.execute(select(Author).filter(Author.id == author_id)).first()
    follower = connection.execute(
        select(Author).filter(Author.id == follower_id)
    ).first()
    if follower and author:
        await update_follows_for_user(
            connection, follower.user, "author", author, is_insert
        )


async def handle_topic_follower_change(connection, topic_id, follower_id, is_insert):
    topic = connection.execute(select(Topic).filter(Topic.id == topic_id)).first()
    follower = connection.execute(
        select(Author).filter(Author.id == follower_id)
    ).first()
    if follower and topic:
        await update_follows_for_user(
            connection, follower.user, "topic", topic, is_insert
        )


@event.listens_for(TopicFollower, "after_insert")
async def after_topic_follower_insert(mapper, connection, target):
    await handle_topic_follower_change(connection, target.topic, target.follower, True)


@event.listens_for(TopicFollower, "after_delete")
async def after_topic_follower_delete(mapper, connection, target):
    await handle_topic_follower_change(connection, target.topic, target.follower, False)


@event.listens_for(AuthorFollower, "after_insert")
async def after_author_follower_insert(mapper, connection, target):
    await handle_author_follower_change(
        connection, target.author, target.follower, True
    )


@event.listens_for(AuthorFollower, "after_delete")
async def after_author_follower_delete(mapper, connection, target):
    await handle_author_follower_change(
        connection, target.author, target.follower, False
    )
