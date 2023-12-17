import time  # For Unix timestamps
from sqlalchemy import and_, select
from sqlalchemy.orm import joinedload

from orm.author import Author
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from orm.shout import Shout, ShoutAuthor, ShoutTopic, ShoutVisibility
from orm.topic import Topic
from resolvers.reaction import reactions_follow, reactions_unfollow
from services.notify import notify_shout


@query.field("get_shouts_drafts")
@login_required
async def get_shouts_drafts(_, info):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            q = (
                select(Shout)
                .options(
                    # joinedload(Shout.created_by, Author.id == Shout.created_by),
                    joinedload(Shout.authors),
                    joinedload(Shout.topics),
                )
                .filter(and_(Shout.deleted_at.is_(None), Shout.created_by == author.id))
            )
            q = q.group_by(Shout.id)
            shouts = []
            for [shout] in session.execute(q).unique():
                shouts.append(shout)
            return shouts


@mutation.field("create_shout")
@login_required
async def create_shout(_, info, inp):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        shout_dict = None
        if author:
            topics = session.query(Topic).filter(Topic.slug.in_(inp.get("topics", []))).all()
            current_time = int(time.time())
            new_shout = Shout(
                **{
                    "title": inp.get("title"),
                    "subtitle": inp.get("subtitle"),
                    "lead": inp.get("lead"),
                    "description": inp.get("description"),
                    "body": inp.get("body", ""),
                    "layout": inp.get("layout"),
                    "created_by": author.id,
                    "authors": [],
                    "slug": inp.get("slug") or f"draft-{time.time()}",
                    "topics": inp.get("topics"),
                    "visibility": ShoutVisibility.AUTHORS.value,
                    "created_at": current_time,  # Set created_at as Unix timestamp
                }
            )
            for topic in topics:
                t = ShoutTopic(topic=topic.id, shout=new_shout.id)
                session.add(t)
            # NOTE: shout made by one author
            sa = ShoutAuthor(shout=new_shout.id, author=author.id)
            session.add(sa)
            shout_dict = new_shout.dict()
            session.add(new_shout)
            reactions_follow(author.id, new_shout.id, True)
            session.commit()

            await notify_shout(shout_dict, "create")
        return {"shout": shout_dict}


@mutation.field("update_shout")
@login_required
async def update_shout(_, info, shout_id, shout_input=None, publish=False):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        shout_dict = None
        current_time = int(time.time())
        if author:
            shout = (
                session.query(Shout)
                .options(
                    # joinedload(Shout.created_by, Author.id == Shout.created_by),
                    joinedload(Shout.authors),
                    joinedload(Shout.topics),
                )
                .filter(Shout.id == shout_id)
                .first()
            )
            if not shout:
                return {"error": "shout not found"}
            if shout.created_by != author.id and author.id not in shout.authors:
                return {"error": "access denied"}
            if shout_input is not None:
                topics_input = shout_input["topics"]
                del shout_input["topics"]
                new_topics_to_link = []
                new_topics = [topic_input for topic_input in topics_input if topic_input["id"] < 0]
                for new_topic in new_topics:
                    del new_topic["id"]
                    created_new_topic = Topic(**new_topic)
                    session.add(created_new_topic)
                    new_topics_to_link.append(created_new_topic)
                if len(new_topics) > 0:
                    session.commit()
                for new_topic_to_link in new_topics_to_link:
                    created_unlinked_topic = ShoutTopic(shout=shout.id, topic=new_topic_to_link.id)
                    session.add(created_unlinked_topic)
                existing_topics_input = [topic_input for topic_input in topics_input if topic_input.get("id", 0) > 0]
                existing_topic_to_link_ids = [
                    existing_topic_input["id"]
                    for existing_topic_input in existing_topics_input
                    if existing_topic_input["id"] not in [topic.id for topic in shout.topics]
                ]
                for existing_topic_to_link_id in existing_topic_to_link_ids:
                    created_unlinked_topic = ShoutTopic(shout=shout.id, topic=existing_topic_to_link_id)
                    session.add(created_unlinked_topic)
                topic_to_unlink_ids = [
                    topic.id
                    for topic in shout.topics
                    if topic.id not in [topic_input["id"] for topic_input in existing_topics_input]
                ]
                shout_topics_to_remove = session.query(ShoutTopic).filter(
                    and_(
                        ShoutTopic.shout == shout.id,
                        ShoutTopic.topic.in_(topic_to_unlink_ids),
                    )
                )
                for shout_topic_to_remove in shout_topics_to_remove:
                    session.delete(shout_topic_to_remove)

                # Replace datetime with Unix timestamp
                shout_input["updated_at"] = current_time  # Set updated_at as Unix timestamp
                Shout.update(shout, shout_input)
                session.add(shout)

                # main topic
                # TODO: test main_topic update
                if "main_topic" in shout_input:
                    old_main_topic = (
                        session.query(ShoutTopic)
                        .filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.main == True))
                        .first()
                    )
                    main_topic = session.query(Topic).filter(Topic.slug == shout_input["main_topic"]).first()
                    new_main_topic = (
                        session.query(ShoutTopic)
                        .filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.topic == main_topic.id))
                        .first()
                    )
                    if old_main_topic is not new_main_topic:
                        old_main_topic.main = False
                        new_main_topic.main = True
                        session.add(old_main_topic)
                        session.add(new_main_topic)

                session.commit()

            if publish:
                if shout.visibility is ShoutVisibility.AUTHORS.value:
                    shout_dict = shout.dict()
                    shout_dict["visibility"] = ShoutVisibility.COMMUNITY.value
                    shout_dict["published_at"] = current_time  # Set published_at as Unix timestamp
                    Shout.update(shout, shout_dict)
                    session.add(shout)
                    await notify_shout(shout.dict(), "public")
            shout_dict = shout.dict()
            session.commit()
            if not publish:
                await notify_shout(shout_dict, "update")
        return {"shout": shout_dict}


@mutation.field("delete_shout")
@login_required
async def delete_shout(_, info, shout_id):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.id == user_id).first()
        shout = session.query(Shout).filter(Shout.id == shout_id).first()
        if not shout:
            return {"error": "invalid shout id"}
        if author:
            if shout.created_by != author.id and author.id not in shout.authors:
                return {"error": "access denied"}
            for author_id in shout.authors:
                reactions_unfollow(author_id, shout_id)
            # Replace datetime with Unix timestamp
            current_time = int(time.time())
            shout_dict = shout.dict()
            shout_dict["deleted_at"] = current_time  # Set deleted_at as Unix timestamp
            Shout.update(shout, shout_dict)
            session.add(shout)
            session.commit()
            await notify_shout(shout_dict, "delete")
    return {}
