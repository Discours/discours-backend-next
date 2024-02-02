import time  # For Unix timestamps

from sqlalchemy import and_, select
from sqlalchemy.orm import joinedload

from orm.author import Author
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic, ShoutVisibility
from orm.topic import Topic
from resolvers.follower import reactions_follow, reactions_unfollow
from resolvers.rater import is_negative, is_positive
from services.auth import login_required
from services.db import local_session
from services.diff import apply_diff, get_diff
from services.notify import notify_shout
from services.schema import mutation, query
from services.search import search_service


@query.field('get_shouts_drafts')
@login_required
async def get_shouts_drafts(_, info):
    user_id = info.context['user_id']
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


@mutation.field('create_shout')
@login_required
async def create_shout(_, info, inp):
    user_id = info.context['user_id']
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        shout_dict = None
        if author:
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
                'visibility': ShoutVisibility.AUTHORS.value,
                'created_at': current_time,  # Set created_at as Unix timestamp
            }

            new_shout = Shout(**shout_dict)
            session.add(new_shout)
            session.commit()

            # NOTE: shout made by one author
            shout = session.query(Shout).where(Shout.slug == slug).first()
            if shout:
                shout_dict = shout.dict()
                sa = ShoutAuthor(shout=shout.id, author=author.id)
                session.add(sa)

                topics = session.query(Topic).filter(Topic.slug.in_(inp.get('topics', []))).all()
                for topic in topics:
                    t = ShoutTopic(topic=topic.id, shout=shout.id)
                    session.add(t)

                reactions_follow(author.id, shout.id, True)

                # notifier
                await notify_shout(shout_dict, 'create')
        return {'shout': shout_dict}


@mutation.field('update_shout')
@login_required
async def update_shout(  # noqa: C901
    _, info, shout_id, shout_input=None, publish=False
):
    user_id = info.context['user_id']
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
                return {'error': 'shout not found'}
            if shout.created_by is not author.id and author.id not in shout.authors:
                return {'error': 'access denied'}
            if shout_input is not None:
                topics_input = shout_input['topics']
                del shout_input['topics']
                new_topics_to_link = []
                new_topics = [topic_input for topic_input in topics_input if topic_input['id'] < 0]
                for new_topic in new_topics:
                    del new_topic['id']
                    created_new_topic = Topic(**new_topic)
                    session.add(created_new_topic)
                    new_topics_to_link.append(created_new_topic)
                if len(new_topics) > 0:
                    session.commit()
                for new_topic_to_link in new_topics_to_link:
                    created_unlinked_topic = ShoutTopic(shout=shout.id, topic=new_topic_to_link.id)
                    session.add(created_unlinked_topic)
                existing_topics_input = [topic_input for topic_input in topics_input if topic_input.get('id', 0) > 0]
                existing_topic_to_link_ids = [
                    existing_topic_input['id']
                    for existing_topic_input in existing_topics_input
                    if existing_topic_input['id'] not in [topic.id for topic in shout.topics]
                ]
                for existing_topic_to_link_id in existing_topic_to_link_ids:
                    created_unlinked_topic = ShoutTopic(shout=shout.id, topic=existing_topic_to_link_id)
                    session.add(created_unlinked_topic)
                topic_to_unlink_ids = [
                    topic.id
                    for topic in shout.topics
                    if topic.id not in [topic_input['id'] for topic_input in existing_topics_input]
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
                shout_input['updated_at'] = current_time  # Set updated_at as Unix timestamp
                Shout.update(shout, shout_input)
                session.add(shout)

                # main topic
                if 'main_topic' in shout_input:
                    old_main_topic = (
                        session.query(ShoutTopic)
                        .filter(and_(ShoutTopic.shout == shout.id, ShoutTopic.main == True))
                        .first()
                    )
                    main_topic = session.query(Topic).filter(Topic.slug == shout_input['main_topic']).first()
                    if isinstance(main_topic, Topic):
                        new_main_topic = (
                            session.query(ShoutTopic)
                            .filter(
                                and_(
                                    ShoutTopic.shout == shout.id,
                                    ShoutTopic.topic == main_topic.id,
                                )
                            )
                            .first()
                        )
                        if (
                            isinstance(old_main_topic, ShoutTopic)
                            and isinstance(new_main_topic, ShoutTopic)
                            and old_main_topic is not new_main_topic
                        ):
                            ShoutTopic.update(old_main_topic, {'main': False})
                            session.add(old_main_topic)
                            ShoutTopic.update(new_main_topic, {'main': True})
                            session.add(new_main_topic)

            shout_dict = shout.dict()
            session.commit()

            if not publish:
                await notify_shout(shout_dict, 'update')
            else:
                await notify_shout(shout_dict, 'published')
                # search service indexing
                search_service.index(shout)

        return {'shout': shout_dict}


@mutation.field('delete_shout')
@login_required
async def delete_shout(_, info, shout_id):
    user_id = info.context['user_id']
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        shout = session.query(Shout).filter(Shout.id == shout_id).first()
        if not shout:
            return {'error': 'invalid shout id'}
        if isinstance(author, Author) and isinstance(shout, Shout):
            # TODO: add editor role allowed here
            if shout.created_by is not author.id and author.id not in shout.authors:
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
        # Proposal accepting logic
        replied_reaction = session.query(Reaction).filter(Reaction.id == r.reply_to).first()
        if replied_reaction and replied_reaction.kind is ReactionKind.PROPOSE.value and replied_reaction.quote:
            # patch all the proposals' quotes
            proposals = (
                session.query(Reaction)
                .filter(and_(Reaction.shout == r.shout, Reaction.kind == ReactionKind.PROPOSE.value))
                .all()
            )
            for proposal in proposals:
                if proposal.quote:
                    proposal_diff = get_diff(shout.body, proposal.quote)
                    proposal_dict = proposal.dict()
                    proposal_dict['quote'] = apply_diff(replied_reaction.quote, proposal_diff)
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
