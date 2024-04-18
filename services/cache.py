import json


from orm.author import Author
from orm.topic import Topic
from services.encoders import CustomJSONEncoder
from services.rediscache import redis

DEFAULT_FOLLOWS = {
    "topics": [],
    "authors": [],
    "communities": [{"id": 1, "name": "Дискурс", "slug": "discours", "pic": ""}],
}


async def cache_author(author: dict):
    payload = json.dumps(author, cls=CustomJSONEncoder)
    await redis.execute("SET", f'user:{author.get("user")}', payload)
    await redis.execute("SET", f'author:{author.get("id")}', payload)

    # update stat all field for followers' caches in <authors> list
    followers_str = await redis.execute("GET", f'author:{author.get("id")}:followers')
    followers = []
    if isinstance(followers_str, str):
        followers = json.loads(followers_str)
    if isinstance(followers, list):
        for follower in followers:
            follower_follows_authors = []
            follower_follows_authors_str = await redis.execute(
                "GET", f'author:{author.get("id")}:follows-authors'
            )
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
    follows_str = await redis.execute(
        "GET", f'author:{author.get("id")}:follows-authors'
    )
    follows_authors = []
    if isinstance(follows_str, str):
        follows_authors = json.loads(follows_str)
    if isinstance(follows_authors, list):
        for followed_author in follows_authors:
            followed_author_followers = []
            followed_author_followers_str = await redis.execute(
                "GET", f'author:{author.get("id")}:followers'
            )
            if isinstance(followed_author_followers_str, str):
                followed_author_followers = json.loads(followed_author_followers_str)
                c = 0
                for old_follower in followed_author_followers:
                    if int(old_follower.get("id")) == int(author.get("id", 0)):
                        followed_author_followers[c] = author
                        break  # exit the loop since we found and updated the author
                    c += 1
            else:
                # author not found in the list, so add the new author with the updated stat field
                followed_author_followers.append(author)


async def cache_follows(follower: Author, entity_type: str, entity, is_insert=True):
    # prepare
    follows = []
    redis_key = f"author:{follower.id}:follows-{entity_type}s"
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
    updated_data = [t.dict() if isinstance(t, Topic) else t for t in follows]
    payload = json.dumps(updated_data, cls=CustomJSONEncoder)
    await redis.execute("SET", redis_key, payload)

    # update follower's stats everywhere
    author_str = await redis.execute("GET", f"author:{follower.id}")
    if isinstance(author_str, str):
        author = json.loads(author_str)
        author["stat"][f"{entity_type}s"] = len(updated_data)
        await cache_author(author)
    return follows


async def cache_follower(follower: Author, author: Author, is_insert=True):
    redis_key = f"author:{author.id}:followers"
    followers_str = await redis.execute("GET", redis_key)
    followers = []
    if isinstance(followers_str, str):
        followers = json.loads(followers_str)
    if is_insert:
        # Remove the entity from followers
        followers = [e for e in followers if e["id"] != author.id]
    else:
        followers.append(follower)
        updated_followers = [
            f.dict() if isinstance(f, Author) else f for f in followers
        ]
        payload = json.dumps(updated_followers, cls=CustomJSONEncoder)
        await redis.execute("SET", redis_key, payload)
        author_str = await redis.execute("GET", f"author:{follower.id}")
        if isinstance(author_str, str):
            author = json.loads(author_str)
            author["stat"]["followers"] = len(updated_followers)
            await cache_author(author)
    return followers
