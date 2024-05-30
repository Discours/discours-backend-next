import json

from orm.topic import TopicFollower
from services.db import local_session
from services.encoders import CustomJSONEncoder
from services.rediscache import redis

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


async def cache_author(author: dict):
    author_id = author.get("id")
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute("SET", f'user:{author.get("user")}', payload)
    await redis.execute("SET", f"author:{author_id}", payload)

    # update stat all field for followers' caches in <authors> list
    followers_str = await redis.execute("GET", f"author:{author_id}:followers")
    followers = []
    if isinstance(followers_str, str):
        followers = json.loads(followers_str)
    if isinstance(followers, list):
        for follower in followers:
            follower_follows_authors = []
            follower_follows_authors_str = await redis.execute("GET", f"author:{author_id}:follows-authors")
            if isinstance(follower_follows_authors_str, str):
                follower_follows_authors = json.loads(follower_follows_authors_str)
                c = 0
                for old_author in follower_follows_authors:
                    if int(old_author.get("id")) == int(author.get("id", 0)):
                        follower_follows_authors[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            else:
                # author not found in the list, so add the new author with the updated stat field
                follower_follows_authors.append(author)

    # update stat field for all authors' caches in <followers> list
    follows_str = await redis.execute("GET", f"author:{author_id}:follows-authors")
    follows_authors = []
    if isinstance(follows_str, str):
        follows_authors = json.loads(follows_str)
    if isinstance(follows_authors, list):
        for followed_author in follows_authors:
            followed_author_followers = []
            followed_author_followers_str = await redis.execute("GET", f"author:{author_id}:followers")
            if isinstance(followed_author_followers_str, str):
                followed_author_followers = json.loads(followed_author_followers_str)
                c = 0
                for old_follower in followed_author_followers:
                    old_follower_id = int(old_follower.get("id"))
                    if old_follower_id == author_id:
                        followed_author_followers[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            # author not found in the list, so add the new author with the updated stat field
            followed_author_followers.append(author)
            await redis.execute(
                "SET",
                f"author:{author_id}:followers",
                json.dumps(followed_author_followers, cls=CustomJSONEncoder),
            )


async def cache_follows(follower: dict, entity_type: str, entity: dict, is_insert=True):
    # prepare
    follows = []
    follower_id = follower.get("id")
    if follower_id:
        redis_key = f"author:{follower_id}:follows-{entity_type}s"
        follows_str = await redis.execute("GET", redis_key)
        if isinstance(follows_str, str):
            follows = json.loads(follows_str)
        if is_insert:
            follows.append(entity)
        else:
            entity_id = entity.get("id")
            if not entity_id:
                raise Exception("wrong entity")
            # Remove the entity from follows
            follows = [e for e in follows if e["id"] != entity_id]

        # update follows cache
        payload = json.dumps(follows, cls=CustomJSONEncoder)
        await redis.execute("SET", redis_key, payload)

        # update follower's stats everywhere
        follower_str = await redis.execute("GET", f"author:{follower_id}")
        if isinstance(follower_str, str):
            follower = json.loads(follower_str)
            follower["stat"][f"{entity_type}s"] = len(follows)
            await cache_author(follower)
    return follows


async def cache_follow_author_change(follower: dict, author: dict, is_insert=True):
    author_id = author.get("id")
    follower_id = follower.get("id")
    followers = []
    if author_id and follower_id:
        redis_key = f"author:{author_id}:followers"
        followers_str = await redis.execute("GET", redis_key)
        followers = json.loads(followers_str) if isinstance(followers_str, str) else []

        # Remove the author from the list of followers, if present
        followers = [f for f in followers if f["id"] != author_id]

        # If inserting, add the new follower to the list if not already present
        if is_insert and not any(f["id"] == follower_id for f in followers):
            followers.append(follower)

        # Remove the follower from the list if not inserting and present
        else:
            followers = [f for f in followers if f["id"] != follower_id]

        # Ensure followers are unique based on their 'id' field
        followers = list({f["id"]: f for f in followers}.values())

        # Update follower's stats everywhere
        follower_str = await redis.execute("GET", f"author:{follower_id}")
        if isinstance(follower_str, str):
            follower = json.loads(follower_str)
            follower["stat"]["followers"] = len(followers)
            await cache_author(follower)

        payload = json.dumps(followers, cls=CustomJSONEncoder)
        await redis.execute("SET", redis_key, payload)

    return followers


async def cache_topic(topic_dict: dict):
    # update stat all field for followers' caches in <topics> list
    followers = local_session().query(TopicFollower).filter(TopicFollower.topic == topic_dict.get("id")).all()
    for tf in followers:
        follower_id = tf.follower
        follower_follows_topics = []
        follower_follows_topics_str = await redis.execute("GET", f"author:{follower_id}:follows-topics")
        if isinstance(follower_follows_topics_str, str):
            follower_follows_topics = json.loads(follower_follows_topics_str)
            c = 0
            for old_topic in follower_follows_topics:
                if int(old_topic.get("id")) == int(topic_dict.get("id", 0)):
                    follower_follows_topics[c] = topic_dict
                    break  # exit the loop since we found and updated the topic
                c += 1
        else:
            # topic not found in the list, so add the new topic with the updated stat field
            follower_follows_topics.append(topic_dict)

        await redis.execute(
            "SET",
            f"author:{follower_id}:follows-topics",
            json.dumps(follower_follows_topics, cls=CustomJSONEncoder),
        )

    # update topic's stat
    topic_dict["stat"]["followers"] = len(followers)

    # save in cache
    payload = json.dumps(topic_dict, cls=CustomJSONEncoder)
    await redis.execute("SET", f'topic:{topic_dict.get("slug")}', payload)
