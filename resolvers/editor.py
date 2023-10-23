from datetime import datetime, timezone

from sqlalchemy import and_, select
from sqlalchemy.orm import joinedload

from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from reaction import reactions_follow, reactions_unfollow
from services.presence import notify_shout


@query.field("loadDrafts")
async def get_drafts(_, info):
    author = info.context["request"].author

    q = (
        select(Shout)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .where(and_(Shout.deletedAt.is_(None), Shout.createdBy == author.id))
    )

    q = q.group_by(Shout.id)

    shouts = []
    with local_session() as session:
        for [shout] in session.execute(q).unique():
            shouts.append(shout)

    return shouts


@mutation.field("createShout")
@login_required
async def create_shout(_, info, inp):
    author_id = info.context["author_id"]
    with local_session() as session:
        topics = (
            session.query(Topic).filter(Topic.slug.in_(inp.get("topics", []))).all()
        )

        new_shout = Shout.create(
            **{
                "title": inp.get("title"),
                "subtitle": inp.get("subtitle"),
                "lead": inp.get("lead"),
                "description": inp.get("description"),
                "body": inp.get("body", ""),
                "layout": inp.get("layout"),
                "authors": inp.get("authors", []),
                "slug": inp.get("slug"),
                "mainTopic": inp.get("mainTopic"),
                "visibility": "authors",
                "createdBy": author_id,
            }
        )

        for topic in topics:
            t = ShoutTopic.create(topic=topic.id, shout=new_shout.id)
            session.add(t)

        # NOTE: shout made by one first author
        sa = ShoutAuthor.create(shout=new_shout.id, author=author_id)
        session.add(sa)

        session.add(new_shout)

        reactions_follow(author_id, new_shout.id, True)

        session.commit()

        # TODO
        # GitTask(inp, user.username, user.email, "new shout %s" % new_shout.slug)

        if new_shout.slug is None:
            new_shout.slug = f"draft-{new_shout.id}"
            session.commit()
        else:
            notify_shout(new_shout.dict(), "create")

    return {"shout": new_shout}


@mutation.field("updateShout")
@login_required
async def update_shout(_, info, shout_id, shout_input=None, publish=False):
    author_id = info.context["author_id"]

    with local_session() as session:
        shout = (
            session.query(Shout)
            .options(
                joinedload(Shout.authors),
                joinedload(Shout.topics),
            )
            .filter(Shout.id == shout_id)
            .first()
        )

        if not shout:
            return {"error": "shout not found"}

        if shout.createdBy != author_id:
            return {"error": "access denied"}

        updated = False

        if shout_input is not None:
            topics_input = shout_input["topics"]
            del shout_input["topics"]

            new_topics_to_link = []
            new_topics = [
                topic_input for topic_input in topics_input if topic_input["id"] < 0
            ]

            for new_topic in new_topics:
                del new_topic["id"]
                created_new_topic = Topic.create(**new_topic)
                session.add(created_new_topic)
                new_topics_to_link.append(created_new_topic)

            if len(new_topics) > 0:
                session.commit()

            for new_topic_to_link in new_topics_to_link:
                created_unlinked_topic = ShoutTopic.create(
                    shout=shout.id, topic=new_topic_to_link.id
                )
                session.add(created_unlinked_topic)

            existing_topics_input = [
                topic_input
                for topic_input in topics_input
                if topic_input.get("id", 0) > 0
            ]
            existing_topic_to_link_ids = [
                existing_topic_input["id"]
                for existing_topic_input in existing_topics_input
                if existing_topic_input["id"]
                not in [topic.id for topic in shout.topics]
            ]

            for existing_topic_to_link_id in existing_topic_to_link_ids:
                created_unlinked_topic = ShoutTopic.create(
                    shout=shout.id, topic=existing_topic_to_link_id
                )
                session.add(created_unlinked_topic)

            topic_to_unlink_ids = [
                topic.id
                for topic in shout.topics
                if topic.id
                not in [topic_input["id"] for topic_input in existing_topics_input]
            ]

            shout_topics_to_remove = session.query(ShoutTopic).filter(
                and_(
                    ShoutTopic.shout == shout.id,
                    ShoutTopic.topic.in_(topic_to_unlink_ids),
                )
            )

            for shout_topic_to_remove in shout_topics_to_remove:
                session.delete(shout_topic_to_remove)

            shout_input["mainTopic"] = shout_input["mainTopic"]["slug"]

            if shout_input["mainTopic"] == "":
                del shout_input["mainTopic"]

            shout.update(shout_input)
            updated = True

        # TODO: use visibility setting

        if publish and shout.visibility == "authors":
            shout.visibility = "community"
            shout.publishedAt = datetime.now(tz=timezone.utc)
            updated = True

            # notify on publish
            notify_shout(shout.dict())

        if updated:
            shout.updatedAt = datetime.now(tz=timezone.utc)

        session.commit()

    # GitTask(inp, user.username, user.email, "update shout %s" % slug)

    notify_shout(shout.dict(), "update")

    return {"shout": shout}


@mutation.field("deleteShout")
@login_required
async def delete_shout(_, info, shout_id):
    author_id = info.context["author_id"]
    with local_session() as session:
        shout = session.query(Shout).filter(Shout.id == shout_id).first()

        if not shout:
            return {"error": "invalid shout id"}

        if author_id != shout.createdBy:
            return {"error": "access denied"}

        for author_id in shout.authors:
            reactions_unfollow(author_id, shout_id)

        shout.deletedAt = datetime.now(tz=timezone.utc)
        session.commit()


        notify_shout(shout.dict(), "delete")

    return {}
