import time
from typing import List

from sqlalchemy import and_, case, desc, func, select, text, asc
from sqlalchemy.orm import aliased, joinedload
from sqlalchemy.sql import union

from orm.author import Author
from orm.rating import RATING_REACTIONS, is_negative, is_positive
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout
from resolvers.editor import handle_proposing
from resolvers.follower import reactions_follow
from services.auth import add_user_role, login_required
from services.db import local_session
from services.notify import notify_reaction
from services.schema import mutation, query
from services.viewed import ViewedStorage
from services.logger import root_logger as logger


def add_reaction_stat_columns(q, aliased_reaction):
    q = q.outerjoin(aliased_reaction).add_columns(
        func.sum(aliased_reaction.id).label('reacted_stat'),
        func.sum(
            case((aliased_reaction.kind == str(ReactionKind.COMMENT.value), 1), else_=0)
        ).label('comments_stat'),
        func.sum(
            case((aliased_reaction.kind == str(ReactionKind.LIKE.value), 1), else_=0)
        ).label('likes_stat'),
        func.sum(
            case((aliased_reaction.kind == str(ReactionKind.DISLIKE.value), 1), else_=0)
        ).label('dislikes_stat'),
        func.max(
            case(
                (aliased_reaction.kind != str(ReactionKind.COMMENT.value), None),
                else_=aliased_reaction.created_at,
            )
        ).label('last_comment_stat'),
    )

    return q


def is_featured_author(session, author_id):
    """checks if author has at least one featured publication"""
    return (
        session.query(Shout)
        .where(Shout.authors.any(id=author_id))
        .filter(and_(Shout.featured_at.is_not(None), Shout.deleted_at.is_(None)))
        .count()
        > 0
    )


def check_to_feature(session, approver_id, reaction):
    """set shout to public if publicated approvers amount > 4"""
    if not reaction.reply_to and is_positive(reaction.kind):
        if is_featured_author(session, approver_id):
            approvers = [approver_id]
            # now count how many approvers are voted already
            reacted_readers = (
                session.query(Reaction).where(Reaction.shout == reaction.shout).all()
            )
            for reacted_reader in reacted_readers:
                if is_featured_author(session, reacted_reader.id):
                    approvers.append(reacted_reader.id)
            if len(approvers) > 4:
                return True
    return False


def check_to_unfeature(session, rejecter_id, reaction):
    """unfeature any shout if 20% of reactions are negative"""
    if not reaction.reply_to and is_negative(reaction.kind):
        if is_featured_author(session, rejecter_id):
            reactions = (
                session.query(Reaction)
                .where(
                    and_(
                        Reaction.shout == reaction.shout,
                        Reaction.kind.in_(RATING_REACTIONS),
                    )
                )
                .all()
            )
            rejects = 0
            for r in reactions:
                approver = (
                    session.query(Author).filter(Author.id == r.created_by).first()
                )
                if is_featured_author(session, approver):
                    if is_negative(r.kind):
                        rejects += 1
            if len(reactions) / rejects < 5:
                return True
    return False


async def set_featured(session, shout_id):
    s = session.query(Shout).where(Shout.id == shout_id).first()
    s.featured_at = int(time.time())
    Shout.update(s, {'featured_at': int(time.time())})
    author = session.query(Author).filter(Author.id == s.created_by).first()
    if author:
        await add_user_role(str(author.user))
    session.add(s)
    session.commit()


def set_unfeatured(session, shout_id):
    s = session.query(Shout).where(Shout.id == shout_id).first()
    Shout.update(s, {'featured_at': None})
    session.add(s)
    session.commit()


async def _create_reaction(session, shout, author, reaction):
    r = Reaction(**reaction)
    session.add(r)
    session.commit()
    rdict = r.dict()

    # collaborative editing
    if (
        rdict.get('reply_to')
        and r.kind in RATING_REACTIONS
        and author.id in shout.authors
    ):
        handle_proposing(session, r, shout)

    # self-regultaion mechanics
    if check_to_unfeature(session, author.id, r):
        set_unfeatured(session, shout.id)
    elif check_to_feature(session, author.id, r):
        await set_featured(session, shout.id)

    # reactions auto-following
    reactions_follow(author.id, reaction['shout'], True)

    rdict['shout'] = shout.dict()
    rdict['created_by'] = author.dict()
    rdict['stat'] = {'commented': 0, 'reacted': 0, 'rating': 0}

    # notifications call
    await notify_reaction(rdict, 'create')

    return rdict


def check_rating(reaction: dict, shout_id: int, session, author: Author):
    kind = reaction.get('kind')
    opposite_kind = (
        ReactionKind.DISLIKE.value if is_positive(kind) else ReactionKind.LIKE.value
    )

    q = select(Reaction).filter(
        and_(
            Reaction.shout == shout_id,
            Reaction.created_by == author.id,
            Reaction.kind.in_(RATING_REACTIONS),
        )
    )
    reply_to = reaction.get('reply_to')
    if reply_to and isinstance(reply_to, int):
        q = q.filter(Reaction.reply_to == reply_to)
    rating_reactions = session.execute(q).all()
    same_rating = filter(
        lambda r: r.created_by == author.id and r.kind == opposite_kind,
        rating_reactions,
    )
    opposite_rating = filter(
        lambda r: r.created_by == author.id and r.kind == opposite_kind,
        rating_reactions,
    )
    if same_rating:
        return {'error': "You can't rate the same thing twice"}
    elif opposite_rating:
        return {'error': 'Remove opposite vote first'}
    elif filter(lambda r: r.created_by == author.id, rating_reactions):
        return {'error': "You can't rate your own thing"}
    return


@mutation.field('create_reaction')
@login_required
async def create_reaction(_, info, reaction):
    user_id = info.context['user_id']

    shout_id = reaction.get('shout')

    if not shout_id:
        return {'error': 'Shout ID is required to create a reaction.'}

    try:
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            author = session.query(Author).filter(Author.user == user_id).first()
            if shout and author:
                reaction['created_by'] = author.id
                kind = reaction.get('kind')
                shout_id = shout.id

                if not kind and isinstance(reaction.get('body'), str):
                    kind = ReactionKind.COMMENT.value

                if not kind:
                    return {'error': 'cannot create reaction without a kind'}

                if kind in RATING_REACTIONS:
                    result = check_rating(reaction, shout_id, session, author)
                    if result:
                        return result

                rdict = await _create_reaction(session, shout, author, reaction)
                return {'reaction': rdict}
    except Exception as e:
        import traceback

        traceback.print_exc()
        logger.error(f'{type(e).__name__}: {e}')

    return {'error': 'Cannot create reaction.'}


@mutation.field('update_reaction')
@login_required
async def update_reaction(_, info, reaction):
    user_id = info.context.get('user_id')
    roles = info.context.get('roles')
    rid = reaction.get('id')
    if rid and isinstance(rid, int) and user_id and roles:
        del reaction['id']
        with local_session() as session:
            reaction_query = select(Reaction).filter(Reaction.id == rid)
            aliased_reaction = aliased(Reaction)
            reaction_query = add_reaction_stat_columns(reaction_query, aliased_reaction)
            reaction_query = reaction_query.group_by(Reaction.id)

            try:
                [r, reacted_stat, commented_stat, likes_stat, dislikes_stat, _l] = (
                    session.execute(reaction_query).unique().first()
                )

                if not r:
                    return {'error': 'invalid reaction id'}

                author = session.query(Author).filter(Author.user == user_id).first()
                if author:
                    if r.created_by != author.id and 'editor' not in roles:
                        return {'error': 'access denied'}

                    body = reaction.get('body')
                    if body:
                        r.body = body
                    r.updated_at = int(time.time())

                    if r.kind != reaction['kind']:
                        # Определение изменения мнения может быть реализовано здесь
                        pass

                    Reaction.update(r, reaction)
                    session.add(r)
                    session.commit()

                    r.stat = {
                        'reacted': reacted_stat,
                        'commented': commented_stat,
                        'rating': int(likes_stat or 0) - int(dislikes_stat or 0),
                    }

                    await notify_reaction(r.dict(), 'update')

                    return {'reaction': r}
                else:
                    return {'error': 'not authorized'}
            except Exception:
                import traceback

                traceback.print_exc()
    return {'error': 'cannot create reaction'}


@mutation.field('delete_reaction')
@login_required
async def delete_reaction(_, info, reaction_id: int):
    user_id = info.context.get('user_id')
    roles = info.context('roles', [])
    if isinstance(reaction_id, int) and user_id and isinstance(roles, list):
        with local_session() as session:
            try:
                author = session.query(Author).filter(Author.user == user_id).one()
                r = session.query(Reaction).filter(Reaction.id == reaction_id).one()
                if r and author:
                    if r.created_by != author.id and 'editor' not in roles:
                        return {'error': 'access denied'}

                    if r.kind in [ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]:
                        session.delete(r)
                        session.commit()
                        await notify_reaction(r.dict(), 'delete')
            except Exception as exc:
                return {'error': f'cannot delete reaction: {exc}'}
    return {'error': 'cannot delete reaction'}


def apply_reaction_filters(by, q):
    shout_slug = by.get('shout', None)
    if shout_slug:
        q = q.filter(Shout.slug == shout_slug)

    elif by.get('shouts'):
        q = q.filter(Shout.slug.in_(by.get('shouts', [])))

    created_by = by.get('created_by', None)
    if created_by:
        q = q.filter(Author.id == created_by)

    topic = by.get('topic', None)
    if topic:
        q = q.filter(Shout.topics.contains(topic))

    if by.get('comment', False):
        q = q.filter(Reaction.kind == ReactionKind.COMMENT.value)
    if by.get('rating', False):
        q = q.filter(Reaction.kind.in_(RATING_REACTIONS))

    by_search = by.get('search', '')
    if len(by_search) > 2:
        q = q.filter(Reaction.body.ilike(f'%{by_search}%'))

    after = by.get('after', None)
    if isinstance(after, int):
        q = q.filter(Reaction.created_at > after)

    return q


@query.field('load_reactions_by')
async def load_reactions_by(_, info, by, limit=50, offset=0):
    """
    :param info: graphql meta
    :param by: {
        :shout - filter by slug
        :shouts - filer by shout slug list
        :created_by - to filter by author
        :topic - to filter by topic
        :search - to search by reactions' body
        :comment - true if body.length > 0
        :after - amount of time ago
        :sort - a fieldname to sort desc by default
    }
    :param limit: int amount of shouts
    :param offset: int offset in this order
    :return: Reaction[]
    """

    q = (
        select(Reaction, Author, Shout)
        .select_from(Reaction)
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    # calculate counters
    aliased_reaction = aliased(Reaction)
    q = add_reaction_stat_columns(q, aliased_reaction)

    # filter
    q = apply_reaction_filters(by, q)
    q = q.where(Reaction.deleted_at.is_(None))

    # group by
    q = q.group_by(Reaction.id, Author.id, Shout.id, aliased_reaction.id)

    # order by
    order_stat = by.get('sort', '').lower()  # 'like' | 'dislike' | 'newest' | 'oldest'
    order_by_stmt = desc(Reaction.created_at)
    if order_stat == 'oldest':
        order_by_stmt = asc(Reaction.created_at)
    elif order_stat.endswith('like'):
        order_by_stmt = desc(f'{order_stat}s_stat')
    q = q.order_by(order_by_stmt)

    # pagination
    q = q.limit(limit).offset(offset)

    reactions = set()
    with local_session() as session:
        result_rows = session.execute(q)
        for [
            reaction,
            author,
            shout,
            reacted_stat,
            commented_stat,
            likes_stat,
            dislikes_stat,
            _last_comment,
        ] in result_rows:
            reaction.created_by = author
            reaction.shout = shout
            reaction.stat = {
                'rating': int(likes_stat or 0) - int(dislikes_stat or 0),
                'reacted': reacted_stat,
                'commented': commented_stat,
            }
            reactions.add(reaction)  # Используем список для хранения реакций

    return reactions


async def reacted_shouts_updates(follower_id: int, limit=50, offset=0) -> List[Shout]:
    shouts: List[Shout] = []
    with local_session() as session:
        author = session.query(Author).filter(Author.id == follower_id).first()
        if author:
            # Shouts where follower is the author
            q1 = (
                select(Shout)
                .outerjoin(
                    Reaction,
                    and_(
                        Reaction.shout_id == Shout.id,
                        Reaction.created_by == follower_id,
                    ),
                )
                .outerjoin(Author, Shout.authors.any(id=follower_id))
                .options(joinedload(Shout.reactions), joinedload(Shout.authors))
            )
            q1 = add_reaction_stat_columns(q1, aliased(Reaction))
            q1 = q1.filter(Author.id == follower_id).group_by(Shout.id)

            # Shouts where follower reacted
            q2 = (
                select(Shout)
                .join(Reaction, Reaction.shout_id == Shout.id)
                .options(joinedload(Shout.reactions), joinedload(Shout.authors))
                .filter(Reaction.created_by == follower_id)
                .group_by(Shout.id)
            )
            q2 = add_reaction_stat_columns(q2, aliased(Reaction))

            # Sort shouts by the `last_comment` field
            combined_query = (
                union(q1, q2)
                .order_by(desc(text('last_comment_stat')))
                .limit(limit)
                .offset(offset)
            )

            results = session.execute(combined_query).scalars()
            for [
                shout,
                reacted_stat,
                commented_stat,
                likes_stat,
                dislikes_stat,
                last_comment,
            ] in results:
                shout.stat = {
                    'viewed': await ViewedStorage.get_shout(shout.slug),
                    'rating': int(likes_stat or 0) - int(dislikes_stat or 0),
                    'reacted': reacted_stat,
                    'commented': commented_stat,
                    'last_comment': last_comment,
                }
                shouts.append(shout)

    return shouts


@query.field('load_shouts_followed')
@login_required
async def load_shouts_followed(_, info, limit=50, offset=0) -> List[Shout]:
    user_id = info.context['user_id']
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            try:
                author_id: int = author.dict()['id']
                shouts = await reacted_shouts_updates(author_id, limit, offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []
