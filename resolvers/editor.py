import time

from sqlalchemy import and_, desc, select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.functions import coalesce

from orm.author import Author
from orm.rating import is_negative, is_positive
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.follower import reactions_follow, reactions_unfollow
from resolvers.stat import get_with_stat
from services.auth import login_required
from services.cache import cache_author, cache_topic
from services.db import local_session
from services.diff import apply_diff, get_diff
from services.logger import root_logger as logger
from services.notify import notify_shout
from services.schema import mutation, query
from services.search import search_service


async def cache_by_id(entity, entity_id: int):
    caching_query = select(entity).filter(entity.id == entity_id)
    [x] = get_with_stat(caching_query)
    if not x:
        return

    d = x.dict()  # convert object to dictionary
    if entity == Author:
        await cache_author(d)
    else:
        await cache_topic(d)
    return d


@query.field("get_my_shout")
@login_required
async def get_my_shout(_, info, shout_id: int):
    logger.debug(info)
    user_id = info.context.get("user_id", "")
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    roles = info.context.get("roles", [])
    shout = None
    if not user_id or not author_id:
        return {"error": "unauthorized", "shout": None}
    with local_session() as session:
        shout = (
            session.query(Shout)
            .filter(Shout.id == shout_id)
            .options(joinedload(Shout.authors), joinedload(Shout.topics))
            .filter(Shout.deleted_at.is_(None))
            .first()
        )
        if not shout:
            return {"error": "no shout found", "shout": None}

        logger.debug(f"got shout authors: {shout.authors} created by {shout.created_by}")
        is_editor = "editor" in roles
        logger.debug(f'viewer is{'' if is_editor else ' not'} editor')
        is_creator = author_id == shout.created_by
        logger.debug(f'viewer is{'' if is_creator else ' not'} creator')
        is_author = bool(list(filter(lambda x: x.id == int(author_id), [x for x in shout.authors])))
        logger.debug(f'viewer is{'' if is_creator else ' not'} author')
        can_edit = is_editor or is_author or is_creator

        if not can_edit:
            return {"error": "forbidden", "shout": None}

        logger.debug("got shout editor with data")
        return {"error": None, "shout": shout}


@query.field("get_shouts_drafts")
@login_required
async def get_shouts_drafts(_, info):
    # user_id = info.context.get("user_id")
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    shouts = []
    with local_session() as session:
        if author_id:
            q = (
                select(Shout)
                .options(joinedload(Shout.authors), joinedload(Shout.topics))
                .filter(and_(Shout.deleted_at.is_(None), Shout.created_by == int(author_id)))
                .filter(Shout.published_at.is_(None))
                .order_by(desc(coalesce(Shout.updated_at, Shout.created_at)))
                .group_by(Shout.id)
            )
            shouts = [shout for [shout] in session.execute(q).unique()]
    return {"shouts": shouts}


@mutation.field("create_shout")
@login_required
async def create_shout(_, info, inp):
    user_id = info.context.get("user_id")
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    if user_id and author_id:
        with local_session() as session:
            author_id = int(author_id)
            current_time = int(time.time())
            slug = inp.get("slug") or f"draft-{current_time}"
            shout_dict = {
                "title": inp.get("title", ""),
                "subtitle": inp.get("subtitle", ""),
                "lead": inp.get("lead", ""),
                "description": inp.get("description", ""),
                "body": inp.get("body", ""),
                "layout": inp.get("layout", "article"),
                "created_by": author_id,
                "authors": [],
                "slug": slug,
                "topics": inp.get("topics", []),
                "published_at": None,
                "created_at": current_time,  # Set created_at as Unix timestamp
            }
            same_slug_shout = session.query(Shout).filter(Shout.slug == shout_dict.get("slug")).first()
            c = 1
            while same_slug_shout is not None:
                same_slug_shout = session.query(Shout).filter(Shout.slug == shout_dict.get("slug")).first()
                c += 1
                shout_dict["slug"] += f"-{c}"
            new_shout = Shout(**shout_dict)
            session.add(new_shout)
            session.commit()

            # NOTE: requesting new shout back
            shout = session.query(Shout).where(Shout.slug == slug).first()
            if shout:
                sa = ShoutAuthor(shout=shout.id, author=author_id)
                session.add(sa)

                topics = session.query(Topic).filter(Topic.slug.in_(inp.get("topics", []))).all()
                for topic in topics:
                    t = ShoutTopic(topic=topic.id, shout=shout.id)
                    session.add(t)

                session.commit()

                reactions_follow(author_id, shout.id, True)

                # notifier
                # await notify_shout(shout_dict, 'create')

                return {"shout": shout}

    return {"error": "cant create shout" if user_id else "unauthorized"}


def patch_main_topic(session, main_topic, shout):
    with session.begin():
        shout = session.query(Shout).options(joinedload(Shout.topics)).filter(Shout.id == shout.id).first()
        if not shout:
            return
        old_main_topic = (
            session.query(ShoutTopic).filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.main.is_(True))).first()
        )

        main_topic = session.query(Topic).filter(Topic.slug == main_topic).first()

        if main_topic:
            new_main_topic = (
                session.query(ShoutTopic)
                .filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.topic == main_topic.id))
                .first()
            )

            if old_main_topic and new_main_topic and old_main_topic is not new_main_topic:
                ShoutTopic.update(old_main_topic, {"main": False})
                session.add(old_main_topic)

                ShoutTopic.update(new_main_topic, {"main": True})
                session.add(new_main_topic)


def patch_topics(session, shout, topics_input):
    new_topics_to_link = [Topic(**new_topic) for new_topic in topics_input if new_topic["id"] < 0]
    if new_topics_to_link:
        session.add_all(new_topics_to_link)
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

    session.query(ShoutTopic).filter(
        and_(ShoutTopic.shout == shout.id, ShoutTopic.topic.in_(topic_to_unlink_ids))
    ).delete(synchronize_session=False)


@mutation.field("update_shout")
@login_required
async def update_shout(_, info, shout_id: int, shout_input=None, publish=False):
    user_id = info.context.get("user_id")
    roles = info.context.get("roles", [])
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    shout_input = shout_input or {}
    current_time = int(time.time())
    shout_id = shout_id or shout_input.get("id", shout_id)
    slug = shout_input.get("slug")
    if not user_id:
        return {"error": "unauthorized"}
    try:
        with local_session() as session:
            if author_id:
                logger.info(f"author for shout#{shout_id} detected author #{author_id}")
                shout_by_id = session.query(Shout).filter(Shout.id == shout_id).first()
                if not shout_by_id:
                    return {"error": "shout not found"}
                if slug != shout_by_id.slug:
                    same_slug_shout = session.query(Shout).filter(Shout.slug == slug).first()
                    c = 1
                    while same_slug_shout is not None:
                        c += 1
                        slug = f"{slug}-{c}"
                        same_slug_shout = session.query(Shout).filter(Shout.slug == slug).first()
                    shout_input["slug"] = slug

                if filter(lambda x: x.id == author_id, [x for x in shout_by_id.authors]) or "editor" in roles:
                    # topics patch
                    topics_input = shout_input.get("topics")
                    if topics_input:
                        patch_topics(session, shout_by_id, topics_input)
                        del shout_input["topics"]
                        for tpc in topics_input:
                            await cache_by_id(Topic, tpc["id"])

                    # main topic
                    main_topic = shout_input.get("main_topic")
                    if main_topic:
                        patch_main_topic(session, main_topic, shout_by_id)

                    shout_input["updated_at"] = current_time
                    shout_input["published_at"] = current_time if publish else None
                    Shout.update(shout_by_id, shout_input)
                    session.add(shout_by_id)
                    session.commit()

                    shout_dict = shout_by_id.dict()

                    if not publish:
                        await notify_shout(shout_dict, "update")
                    else:
                        await notify_shout(shout_dict, "published")
                        # search service indexing
                        search_service.index(shout_by_id)
                        for a in shout_by_id.authors:
                            await cache_by_id(Author, a.id)

                    return {"shout": shout_dict, "error": None}
                else:
                    return {"error": "access denied", "shout": None}

    except Exception as exc:
        import traceback

        traceback.print_exc()
        logger.error(exc)
        logger.error(f" cannot update with data: {shout_input}")

    return {"error": "cant update shout"}


@mutation.field("delete_shout")
@login_required
async def delete_shout(_, info, shout_id: int):
    user_id = info.context.get("user_id")
    roles = info.context.get("roles", [])
    author_dict = info.context.get("author")
    if not author_dict:
        return {"error": "author profile was not found"}
    author_id = author_dict.get("id")
    if user_id and author_id:
        author_id = int(author_id)
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            if not isinstance(shout, Shout):
                return {"error": "invalid shout id"}
            shout_dict = shout.dict()
            # NOTE: only owner and editor can mark the shout as deleted
            if shout_dict["created_by"] == author_id or "editor" in roles:
                shout_dict["deleted_at"] = int(time.time())
                Shout.update(shout, shout_dict)
                session.add(shout)
                session.commit()

                for author in shout.authors:
                    reactions_unfollow(author.id, shout_id)
                    await cache_by_id(Author, author.id)

                for topic in shout.topics:
                    await cache_by_id(Topic, topic.id)

                await notify_shout(shout_dict, "delete")
                return {"error": None}
            else:
                return {"error": "access denied"}


def handle_proposing(session, r, shout):
    if is_positive(r.kind):
        replied_reaction = session.query(Reaction).filter(Reaction.id == r.reply_to, Reaction.shout == r.shout).first()

        if replied_reaction and replied_reaction.kind is ReactionKind.PROPOSE.value and replied_reaction.quote:
            # patch all the proposals' quotes
            proposals = (
                session.query(Reaction)
                .filter(
                    and_(
                        Reaction.shout == r.shout,
                        Reaction.kind == ReactionKind.PROPOSE.value,
                    )
                )
                .all()
            )

            for proposal in proposals:
                if proposal.quote:
                    proposal_diff = get_diff(shout.body, proposal.quote)
                    proposal_dict = proposal.dict()
                    proposal_dict["quote"] = apply_diff(replied_reaction.quote, proposal_diff)
                    Reaction.update(proposal, proposal_dict)
                    session.add(proposal)

            # patch shout's body
            shout_dict = shout.dict()
            shout_dict["body"] = replied_reaction.quote
            Shout.update(shout, shout_dict)
            session.add(shout)
            session.commit()

    if is_negative(r.kind):
        # TODO: rejection logic
        pass
