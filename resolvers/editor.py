import time

from sqlalchemy import and_, select, desc
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.functions import coalesce

from orm.author import Author
from orm.rating import is_negative, is_positive
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic
from resolvers.follower import reactions_follow, reactions_unfollow
from services.auth import login_required
from services.db import local_session
from services.diff import apply_diff, get_diff
from services.notify import notify_shout
from services.schema import mutation, query
from services.search import search_service
from services.logger import root_logger as logger


@query.field('get_shouts_drafts')
@login_required
async def get_shouts_drafts(_, info):
    user_id = info.context['user_id']
    shouts = []
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            q = (
                select(Shout)
                .options(joinedload(Shout.authors), joinedload(Shout.topics))
                .filter(and_(Shout.deleted_at.is_(None), Shout.created_by == author.id))
                .filter(Shout.published_at.is_(None))
                .order_by(desc(coalesce(Shout.updated_at, Shout.created_at)))
                .group_by(Shout.id)
            )
            shouts = [shout for [shout] in session.execute(q).unique()]
    return shouts


@mutation.field('create_shout')
@login_required
async def create_shout(_, info, inp):
    user_id = info.context.get('user_id')
    if user_id:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            if isinstance(author, Author):
                current_time = int(time.time())
                slug = inp.get('slug') or f'draft-{current_time}'
                shout_dict = {
                    'title': inp.get('title', ''),
                    'subtitle': inp.get('subtitle', ''),
                    'lead': inp.get('lead', ''),
                    'description': inp.get('description', ''),
                    'body': inp.get('body', ''),
                    'layout': inp.get('layout', 'article'),
                    'created_by': author.id,
                    'authors': [],
                    'slug': slug,
                    'topics': inp.get('topics', []),
                    'published_at': None,
                    'created_at': current_time,  # Set created_at as Unix timestamp
                }
                same_slug_shout = (
                    session.query(Shout)
                    .filter(Shout.slug == shout_dict.get('slug'))
                    .first()
                )
                c = 1
                while same_slug_shout is not None:
                    same_slug_shout = (
                        session.query(Shout)
                        .filter(Shout.slug == shout_dict.get('slug'))
                        .first()
                    )
                    c += 1
                    shout_dict['slug'] += f'-{c}'
                new_shout = Shout(**shout_dict)
                session.add(new_shout)
                session.commit()

                # NOTE: requesting new shout back
                shout = session.query(Shout).where(Shout.slug == slug).first()
                if shout:
                    sa = ShoutAuthor(shout=shout.id, author=author.id)
                    session.add(sa)

                    topics = (
                        session.query(Topic)
                        .filter(Topic.slug.in_(inp.get('topics', [])))
                        .all()
                    )
                    for topic in topics:
                        t = ShoutTopic(topic=topic.id, shout=shout.id)
                        session.add(t)

                    session.commit()

                    reactions_follow(author.id, shout.id, True)

                    # notifier
                    # await notify_shout(shout_dict, 'create')

                    return {'shout': shout}

    return {'error': 'cant create shout' if user_id else 'unauthorized'}


def patch_main_topic(session, main_topic, shout):
    old_main_topic = (
        session.query(ShoutTopic)
        .filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.main.is_(True)))
        .first()
    )

    main_topic = session.query(Topic).filter(Topic.slug == main_topic).first()

    if main_topic:
        new_main_topic = (
            session.query(ShoutTopic)
            .filter(
                and_(ShoutTopic.shout == shout.id, ShoutTopic.topic == main_topic.id)
            )
            .first()
        )

        if old_main_topic and new_main_topic and old_main_topic is not new_main_topic:
            ShoutTopic.update(old_main_topic, {'main': False})
            session.add(old_main_topic)

            ShoutTopic.update(new_main_topic, {'main': True})
            session.add(new_main_topic)


def patch_topics(session, shout, topics_input):
    new_topics_to_link = [
        Topic(**new_topic) for new_topic in topics_input if new_topic['id'] < 0
    ]
    if new_topics_to_link:
        session.add_all(new_topics_to_link)
        session.commit()

    for new_topic_to_link in new_topics_to_link:
        created_unlinked_topic = ShoutTopic(shout=shout.id, topic=new_topic_to_link.id)
        session.add(created_unlinked_topic)

    existing_topics_input = [
        topic_input for topic_input in topics_input if topic_input.get('id', 0) > 0
    ]
    existing_topic_to_link_ids = [
        existing_topic_input['id']
        for existing_topic_input in existing_topics_input
        if existing_topic_input['id'] not in [topic.id for topic in shout.topics]
    ]

    for existing_topic_to_link_id in existing_topic_to_link_ids:
        created_unlinked_topic = ShoutTopic(
            shout=shout.id, topic=existing_topic_to_link_id
        )
        session.add(created_unlinked_topic)

    topic_to_unlink_ids = [
        topic.id
        for topic in shout.topics
        if topic.id not in [topic_input['id'] for topic_input in existing_topics_input]
    ]

    session.query(ShoutTopic).filter(
        and_(ShoutTopic.shout == shout.id, ShoutTopic.topic.in_(topic_to_unlink_ids))
    ).delete(synchronize_session=False)


@mutation.field('update_shout')
@login_required
async def update_shout(_, info, shout_id, shout_input=None, publish=False):
    try:
        user_id = info.context.get('user_id')
        if not user_id:
            return {"error": "unauthorized"}
        roles = info.context.get('roles', [])
        shout_input = shout_input or {}
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            current_time = int(time.time())
            shout_id = shout_id or shout_input.get('id')
            slug = shout_input.get('slug')
            if slug:

                shout_by_id = session.query(Shout).filter(Shout.id == shout_id).first()
                if shout_by_id and slug != shout_by_id.slug:
                    same_slug_shout = (
                        session.query(Shout)
                        .filter(Shout.slug == shout_input.get('slug'))
                        .first()
                    )
                    c = 1
                    while same_slug_shout is not None:
                        c += 1
                        slug += f'-{c}'
                        same_slug_shout = (
                            session.query(Shout)
                            .filter(Shout.slug == slug)  # Use the updated slug value here
                            .first()
                        )
                    shout_input['slug'] = slug

            if isinstance(author, Author) and isinstance(shout_id, int):
                shout = (
                    session.query(Shout)
                    .options(joinedload(Shout.authors), joinedload(Shout.topics))
                    .filter(Shout.id == shout_id)
                    .first()
                )

                if not shout:
                    return {'error': 'shout not found'}
                if (
                    shout.created_by != author.id
                    and not filter(lambda x: x == author.id, shout.authors)
                    and 'editor' not in roles
                ):
                    return {'error': 'access denied'}

                # topics patch
                topics_input = shout_input.get('topics')
                if topics_input:
                    patch_topics(session, shout, topics_input)
                    del shout_input['topics']

                # main topic
                main_topic = shout_input.get('main_topic')
                if main_topic:
                    patch_main_topic(session, main_topic, shout)

                shout_input['updated_at'] = current_time
                shout_input['published_at'] = current_time if publish else None
                Shout.update(shout, shout_input)
                session.add(shout)
                session.commit()

                shout_dict = shout.dict()

                if not publish:
                    await notify_shout(shout_dict, 'update')
                else:
                    await notify_shout(shout_dict, 'published')
                    # search service indexing
                    search_service.index(shout)

                return {'shout': shout_dict}
    except Exception as exc:
        logger.error(exc)
        logger.error(f' cannot update with data: {shout_input}')

    return {'error': 'cant update shout'}


@mutation.field('delete_shout')
@login_required
async def delete_shout(_, info, shout_id):
    user_id = info.context.get('user_id')
    roles = info.context.get('roles')
    if user_id:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            if not shout:
                return {'error': 'invalid shout id'}
            if author and shout:
                if (
                    shout.created_by is not author.id
                    and author.id not in shout.authors
                    and 'editor' not in roles
                ):
                    return {'error': 'access denied'}

                for author_id in shout.authors:
                    reactions_unfollow(author_id, shout_id)

                shout_dict = shout.dict()
                shout_dict['deleted_at'] = int(time.time())
                Shout.update(shout, shout_dict)
                session.add(shout)
                session.commit()
                await notify_shout(shout_dict, 'delete')

    return {}


def handle_proposing(session, r, shout):
    if is_positive(r.kind):
        replied_reaction = (
            session.query(Reaction)
            .filter(Reaction.id == r.reply_to, Reaction.shout == r.shout)
            .first()
        )

        if (
            replied_reaction
            and replied_reaction.kind is ReactionKind.PROPOSE.value
            and replied_reaction.quote
        ):
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
                    proposal_dict['quote'] = apply_diff(
                        replied_reaction.quote, proposal_diff
                    )
                    Reaction.update(proposal, proposal_dict)
                    session.add(proposal)

            # patch shout's body
            shout_dict = shout.dict()
            shout_dict['body'] = replied_reaction.quote
            Shout.update(shout, shout_dict)
            session.add(shout)
            session.commit()

    if is_negative(r.kind):
        # TODO: rejection logic
        pass
