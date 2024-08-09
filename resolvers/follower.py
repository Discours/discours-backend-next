from typing import List

from sqlalchemy import select
from sqlalchemy.sql import and_

from cache.cache import (
    cache_author,
    cache_topic,
    get_cached_follower_authors,
    get_cached_follower_topics,
)
from orm.author import Author, AuthorFollower
from orm.community import Community, CommunityFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.db import local_session
from services.notify import notify_follower
from services.schema import mutation, query


@mutation.field("follow")
@login_required
async def follow(_, info, what, slug):
    user_id = info.context.get("user_id")
    follower_dict = info.context.get("author")
    if not user_id or not follower_dict:
        return {"error": "unauthorized"}
    follower_id = follower_dict.get("id")

    entity_classes = {
        "AUTHOR": (Author, AuthorFollower, get_cached_follower_authors, cache_author),
        "TOPIC": (Topic, TopicFollower, get_cached_follower_topics, cache_topic),
        "COMMUNITY": (Community, CommunityFollower, None, None),  # No cache methods provided for community
        "SHOUT": (Shout, ShoutReactionsFollower, None, None),  # No cache methods provided for shout
    }

    if what not in entity_classes:
        return {"error": "invalid follow type"}

    entity_class, follower_class, get_cached_follows_method, cache_method = entity_classes[what]
    entity_type = what.lower()
    entity_id = None
    entity_dict = None

    try:
        # Fetch entity id from the database
        with local_session() as session:
            entity_query = select(entity_class).filter(entity_class.slug == slug)
            [entity] = get_with_stat(entity_query)
            if not entity:
                return {"error": f"{what.lower()} not found"}
            entity_id = entity.id
            entity_dict = entity.dict()

        if entity_id:
            # Update database
            with local_session() as session:
                sub = follower_class(follower=follower_id, **{entity_type: entity_id})
                session.add(sub)
                session.commit()

            follows = None
            # Update cache
            if cache_method:
                await cache_method(entity_dict)
            if get_cached_follows_method:
                follows = await get_cached_follows_method(follower_id)

            # Notify author (only for AUTHOR type)
            if what == "AUTHOR":
                await notify_follower(follower=follower_dict, author=entity_id, action="follow")

    except Exception as exc:
        return {"error": str(exc)}

    return {f"{what.lower()}s": follows}


@mutation.field("unfollow")
@login_required
async def unfollow(_, info, what, slug):
    user_id = info.context.get("user_id")
    follower_dict = info.context.get("author")
    if not user_id or not follower_dict:
        return {"error": "unauthorized"}
    follower_id = follower_dict.get("id")

    entity_classes = {
        "AUTHOR": (Author, AuthorFollower, get_cached_follower_authors, cache_author),
        "TOPIC": (Topic, TopicFollower, get_cached_follower_topics, cache_topic),
        "COMMUNITY": (Community, CommunityFollower, None, None),  # No cache methods provided for community
        "SHOUT": (
            Shout,
            ShoutReactionsFollower,
            None,
        ),  # No cache methods provided for shout
    }

    if what not in entity_classes:
        return {"error": "invalid unfollow type"}

    entity_class, follower_class, get_cached_follows_method, cache_method = entity_classes[what]
    entity_type = what.lower()
    entity_id = None
    follows = []
    error = None

    try:
        with local_session() as session:
            entity = session.query(entity_class).filter(entity_class.slug == slug).first()
            if not entity:
                return {"error": f"{what.lower()} not found"}
            entity_id = entity.id

            sub = (
                session.query(follower_class)
                .filter(
                    and_(
                        getattr(follower_class, "follower") == follower_id,
                        getattr(follower_class, entity_type) == entity_id,
                    )
                )
                .first()
            )
            if sub:
                session.delete(sub)
                session.commit()

                if cache_method:
                    await cache_method(entity.dict())

                if get_cached_follows_method:
                    follows = await get_cached_follows_method(follower_id)

                if what == "AUTHOR":
                    await notify_follower(follower=follower_dict, author=entity_id, action="unfollow")

    except Exception as exc:
        return {"error": str(exc)}

    return {f"{entity_type}s": follows, "error": error}


@query.field("get_shout_followers")
def get_shout_followers(_, _info, slug: str = "", shout_id: int | None = None) -> List[Author]:
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
