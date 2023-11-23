from services.auth import login_required
from resolvers.author import author_follow, author_unfollow
from resolvers.reaction import reactions_follow, reactions_unfollow
from resolvers.topic import topic_follow, topic_unfollow
from resolvers.community import community_follow, community_unfollow
from services.following import FollowingManager, FollowingResult
from services.db import local_session
from orm.author import Author
from services.notify import notify_follower
from services.schema import mutation


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
