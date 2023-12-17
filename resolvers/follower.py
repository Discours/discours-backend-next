from typing import List

from sqlalchemy import select

from orm.community import Community, CommunityAuthor
from orm.reaction import Reaction
from orm.shout import Shout
from orm.topic import Topic, TopicFollower
from services.auth import login_required
from resolvers.author import author_follow, author_unfollow
from resolvers.reaction import reactions_follow, reactions_unfollow
from resolvers.topic import topic_follow, topic_unfollow
from resolvers.community import community_follow, community_unfollow
from services.following import FollowingManager, FollowingResult
from services.db import local_session
from orm.author import Author, AuthorFollower
from services.notify import notify_follower
from services.schema import mutation, query


@login_required
@mutation.field("follow")
async def follow(_, info, what, slug):
    user_id = info.context["user_id"]
    try:
        with local_session() as session:
            actor = session.query(Author).filter(Author.user == user_id).first()
            if actor:
                follower_id = actor.id
                if what == "AUTHOR":
                    if author_follow(follower_id, slug):
                        result = FollowingResult("NEW", "author", slug)
                        await FollowingManager.push("author", result)
                        author = session.query(Author.id).where(Author.slug == slug).one()
                        follower = session.query(Author).where(Author.id == follower_id).one()
                        await notify_follower(follower.dict(), author.id)
                elif what == "TOPIC":
                    if topic_follow(follower_id, slug):
                        result = FollowingResult("NEW", "topic", slug)
                        await FollowingManager.push("topic", result)
                elif what == "COMMUNITY":
                    if community_follow(follower_id, slug):
                        result = FollowingResult("NEW", "community", slug)
                        await FollowingManager.push("community", result)
                elif what == "REACTIONS":
                    if reactions_follow(follower_id, slug):
                        result = FollowingResult("NEW", "shout", slug)
                        await FollowingManager.push("shout", result)
    except Exception as e:
        print(Exception(e))
        return {"error": str(e)}

    return {}


@login_required
@mutation.field("unfollow")
async def unfollow(_, info, what, slug):
    user_id = info.context["user_id"]
    try:
        with local_session() as session:
            actor = session.query(Author).filter(Author.user == user_id).first()
            if actor:
                follower_id = actor.id
                if what == "AUTHOR":
                    if author_unfollow(follower_id, slug):
                        result = FollowingResult("DELETED", "author", slug)
                        await FollowingManager.push("author", result)
                        author = session.query(Author.id).where(Author.slug == slug).one()
                        follower = session.query(Author).where(Author.id == follower_id).one()
                        await notify_follower(follower.dict(), author.id, "unfollow")
                elif what == "TOPIC":
                    if topic_unfollow(follower_id, slug):
                        result = FollowingResult("DELETED", "topic", slug)
                        await FollowingManager.push("topic", result)
                elif what == "COMMUNITY":
                    if community_unfollow(follower_id, slug):
                        result = FollowingResult("DELETED", "community", slug)
                        await FollowingManager.push("community", result)
                elif what == "REACTIONS":
                    if reactions_unfollow(follower_id, slug):
                        result = FollowingResult("DELETED", "shout", slug)
                        await FollowingManager.push("shout", result)
    except Exception as e:
        return {"error": str(e)}

    return {}


@query.field("get_my_followed")
@login_required
async def get_my_followed(_, info):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            authors_query = select(Author).join(AuthorFollower).filter(AuthorFollower.follower == author.id)

            topics_query = select(Topic).join(TopicFollower).filter(TopicFollower.follower == author.id)

            # communities_query = select(Community).join(CommunityAuthor).filter(CommunityAuthor.author == author.id)

            topics = session.execute(topics_query).scalars().all()
            authors = session.execute(authors_query).scalars().all()
            communities = session.query(Community).scalars().all()

            return {"topics": topics, "authors": authors, "communities": communities}


@query.field("get_shout_followers")
def get_shout_followers(_, _info, slug: str = "", shout_id: int = None) -> List[Author]:
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
