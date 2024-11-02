import time

from sqlalchemy import and_, asc, case, desc, func, select
from sqlalchemy.orm import aliased

from orm.author import Author
from orm.rating import PROPOSAL_REACTIONS, RATING_REACTIONS, is_negative, is_positive
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout
from resolvers.editor import handle_proposing
from resolvers.follower import follow
from resolvers.stat import update_author_stat
from services.auth import add_user_role, login_required
from services.db import local_session
from services.notify import notify_reaction
from services.schema import mutation, query
from utils.logger import root_logger as logger


def query_reactions():
    """
    Base query for fetching reactions with associated authors and shouts.

    :return: Base query.
    """
    return (
        select(
            Reaction,
            Author,
            Shout,
        )
        .select_from(Reaction)
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )


def add_reaction_stat_columns(q):
    """
    Add statistical columns to a reaction query.

    :param q: SQL query for reactions.
    :return: Query with added statistics columns.
    """
    aliased_reaction = aliased(Reaction)
    # Join reactions and add statistical columns
    q = q.outerjoin(
        aliased_reaction,
        and_(
            aliased_reaction.reply_to == Reaction.id,
            aliased_reaction.deleted_at.is_(None),
        ),
    ).add_columns(
        # Count unique comments
        func.coalesce(
            func.count(aliased_reaction.id).filter(aliased_reaction.kind == ReactionKind.COMMENT.value), 0
        ).label("comments_stat"),
        # Calculate rating as the difference between likes and dislikes
        func.sum(
            case(
                (aliased_reaction.kind == ReactionKind.LIKE.value, 1),
                (aliased_reaction.kind == ReactionKind.DISLIKE.value, -1),
                else_=0,
            )
        ).label("rating_stat"),
    )
    return q


def get_reactions_with_stat(q, limit, offset):
    """
    Execute the reaction query and retrieve reactions with statistics.

    :param q: Query with reactions and statistics.
    :param limit: Number of reactions to load.
    :param offset: Pagination offset.
    :return: List of reactions.
    """
    q = q.limit(limit).offset(offset)
    reactions = []

    with local_session() as session:
        result_rows = session.execute(q)
        for reaction, author, shout, commented_stat, rating_stat in result_rows:
            reaction.created_by = author
            reaction.shout = shout
            reaction.stat = {"rating": rating_stat, "comments": commented_stat}
            reactions.append(reaction)

    return reactions


def is_featured_author(session, author_id) -> bool:
    """
    Check if an author has at least one featured article.

    :param session: Database session.
    :param author_id: Author ID.
    :return: True if the author has a featured article, else False.
    """
    return session.query(
        session.query(Shout).where(Shout.authors.any(id=author_id)).filter(Shout.featured_at.is_not(None)).exists()
    ).scalar()


def check_to_feature(session, approver_id, reaction) -> bool:
    """
    Make a shout featured if it receives more than 4 votes.

    :param session: Database session.
    :param approver_id: Approver author ID.
    :param reaction: Reaction object.
    :return: True if shout should be featured, else False.
    """
    if not reaction.reply_to and is_positive(reaction.kind):
        approvers = {approver_id}
        # Count the number of approvers
        reacted_readers = (
            session.query(Reaction.created_by)
            .filter(Reaction.shout == reaction.shout, is_positive(Reaction.kind), Reaction.deleted_at.is_(None))
            .distinct()
        )

        for reader_id in reacted_readers:
            if is_featured_author(session, reader_id):
                approvers.add(reader_id)
        return len(approvers) > 4
    return False


def check_to_unfeature(session, rejecter_id, reaction) -> bool:
    """
    Unfeature a shout if 20% of reactions are negative.

    :param session: Database session.
    :param rejecter_id: Rejecter author ID.
    :param reaction: Reaction object.
    :return: True if shout should be unfeatured, else False.
    """
    if not reaction.reply_to and is_negative(reaction.kind):
        total_reactions = (
            session.query(Reaction)
            .filter(
                Reaction.shout == reaction.shout, Reaction.kind.in_(RATING_REACTIONS), Reaction.deleted_at.is_(None)
            )
            .count()
        )

        negative_reactions = (
            session.query(Reaction)
            .filter(Reaction.shout == reaction.shout, is_negative(Reaction.kind), Reaction.deleted_at.is_(None))
            .count()
        )

        return total_reactions > 0 and (negative_reactions / total_reactions) >= 0.2
    return False


async def set_featured(session, shout_id):
    """
    Feature a shout and update the author's role.

    :param session: Database session.
    :param shout_id: Shout ID.
    """
    s = session.query(Shout).filter(Shout.id == shout_id).first()
    if s:
        current_time = int(time.time())
        s.featured_at = current_time
        session.commit()
        author = session.query(Author).filter(Author.id == s.created_by).first()
        if author:
            await add_user_role(str(author.user))
        session.add(s)
        session.commit()


def set_unfeatured(session, shout_id):
    """
    Unfeature a shout.

    :param session: Database session.
    :param shout_id: Shout ID.
    """
    session.query(Shout).filter(Shout.id == shout_id).update({"featured_at": None})
    session.commit()


async def _create_reaction(session, info, shout, author_id: int, reaction) -> dict:
    """
    Create a new reaction and perform related actions such as updating counters and notification.

    :param session: Database session.
    :param info: GraphQL context info.
    :param shout: Shout object.
    :param author_id: Author ID.
    :param reaction: Dictionary with reaction data.
    :return: Dictionary with created reaction data.
    """
    r = Reaction(**reaction)
    session.add(r)
    session.commit()
    rdict = r.dict()

    # Update author stat for comments
    if r.kind == ReactionKind.COMMENT.value:
        update_author_stat(author_id)

    # Handle proposal
    if r.reply_to and r.kind in PROPOSAL_REACTIONS and author_id in shout.authors:
        handle_proposing(session, r, shout)

    # Handle rating
    if r.kind in RATING_REACTIONS:
        if check_to_unfeature(session, author_id, r):
            set_unfeatured(session, shout.id)
        elif check_to_feature(session, author_id, r):
            await set_featured(session, shout.id)

        # Follow if liked
        if r.kind == ReactionKind.LIKE.value:
            try:
                follow(None, info, "shout", shout.slug)
            except Exception:
                pass

    # Notify creation
    await notify_reaction(rdict, "create")

    return rdict


def prepare_new_rating(reaction: dict, shout_id: int, session, author_id: int):
    """
    Check for the possibility of rating a shout.

    :param reaction: Dictionary with reaction data.
    :param shout_id: Shout ID.
    :param session: Database session.
    :param author_id: Author ID.
    :return: Dictionary with error or None.
    """
    kind = reaction.get("kind")
    opposite_kind = ReactionKind.DISLIKE.value if is_positive(kind) else ReactionKind.LIKE.value

    existing_ratings = (
        session.query(Reaction)
        .filter(
            Reaction.shout == shout_id,
            Reaction.created_by == author_id,
            Reaction.kind.in_(RATING_REACTIONS),
            Reaction.deleted_at.is_(None),
        )
        .all()
    )

    for r in existing_ratings:
        if r.kind == kind:
            return {"error": "You can't rate the same thing twice"}
        if r.kind == opposite_kind:
            return {"error": "Remove opposite vote first"}
    if shout_id in [r.shout for r in existing_ratings]:
        return {"error": "You can't rate your own thing"}

    return


@mutation.field("create_reaction")
@login_required
async def create_reaction(_, info, reaction):
    """
    Create a new reaction through a GraphQL request.

    :param info: GraphQL context info.
    :param reaction: Dictionary with reaction data.
    :return: Dictionary with created reaction data or error.
    """
    author_dict = info.context.get("author", {})
    author_id = author_dict.get("id")
    shout_id = int(reaction.get("shout", "0"))

    logger.debug(f"Creating reaction with data: {reaction}")
    logger.debug(f"Author ID: {author_id}, Shout ID: {shout_id}")

    if not shout_id or not author_id:
        return {"error": "Shout ID and author ID are required to create a reaction."}

    try:
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).first()
            
            logger.debug(f"Loaded shout: {shout and shout.id}")
            
            if shout:
                reaction["created_by"] = author_id
                kind = reaction.get(
                    "kind", ReactionKind.COMMENT.value if isinstance(reaction.get("body"), str) else None
                )
                
                logger.debug(f"Reaction kind: {kind}")

                if kind in RATING_REACTIONS:
                    error_result = prepare_new_rating(reaction, shout_id, session, author_id)
                    if error_result:
                        logger.error(f"Rating preparation error: {error_result}")
                        return error_result

                rdict = await _create_reaction(session, info, shout, author_id, reaction)
                
                logger.debug(f"Created reaction result: {rdict}")

                rdict["created_by"] = author_dict
                return {"reaction": rdict}
            else:
                logger.error(f"Shout not found with ID: {shout_id}")
                return {"error": "Shout not found"}
    except Exception as e:
        logger.error(f"{type(e).__name__}: {e}")
        return {"error": "Cannot create reaction."}


@mutation.field("update_reaction")
@login_required
async def update_reaction(_, info, reaction):
    """
    Update an existing reaction through a GraphQL request.

    :param info: GraphQL context info.
    :param reaction: Dictionary with reaction data.
    :return: Dictionary with updated reaction data or error.
    """
    user_id = info.context.get("user_id")
    roles = info.context.get("roles")
    rid = reaction.get("id")

    if not rid or not user_id or not roles:
        return {"error": "Invalid input data"}

    del reaction["id"]

    with local_session() as session:
        try:
            reaction_query = query_reactions().filter(Reaction.id == rid)
            reaction_query = add_reaction_stat_columns(reaction_query)
            reaction_query = reaction_query.group_by(Reaction.id, Author.id, Shout.id)

            result = session.execute(reaction_query).unique().first()
            if result:
                r, author, shout, commented_stat, rating_stat = result
                if not r or not author:
                    return {"error": "Invalid reaction ID or unauthorized"}

                if r.created_by != author.id and "editor" not in roles:
                    return {"error": "Access denied"}

                # Update reaction
                r.body = reaction.get("body", r.body)
                r.updated_at = int(time.time())
                Reaction.update(r, reaction)
                session.add(r)
                session.commit()

                r.stat = {
                    "commented": commented_stat,
                    "rating": rating_stat,
                }

                await notify_reaction(r.dict(), "update")

                return {"reaction": r}
        except Exception as e:
            logger.error(f"{type(e).__name__}: {e}")
            return {"error": "Cannot update reaction"}


@mutation.field("delete_reaction")
@login_required
async def delete_reaction(_, info, reaction_id: int):
    """
    Delete an existing reaction through a GraphQL request.

    :param info: GraphQL context info.
    :param reaction_id: Reaction ID to delete.
    :return: Dictionary with deleted reaction data or error.
    """
    user_id = info.context.get("user_id")
    author_id = info.context.get("author", {}).get("id")
    roles = info.context.get("roles", [])

    if not user_id:
        return {"error": "Unauthorized"}

    with local_session() as session:
        try:
            author = session.query(Author).filter(Author.user == user_id).one()
            r = session.query(Reaction).filter(Reaction.id == reaction_id).one()

            if r.created_by != author_id and "editor" not in roles:
                return {"error": "Access denied"}

            logger.debug(f"{user_id} user removing his #{reaction_id} reaction")
            reaction_dict = r.dict()
            session.delete(r)
            session.commit()

            # Update author stat
            if r.kind == ReactionKind.COMMENT.value:
                update_author_stat(author.id)

            await notify_reaction(reaction_dict, "delete")

            return {"error": None, "reaction": reaction_dict}
        except Exception as e:
            logger.error(f"{type(e).__name__}: {e}")
            return {"error": "Cannot delete reaction"}


def apply_reaction_filters(by, q):
    """
    Apply filters to a reaction query.

    :param by: Dictionary with filter parameters.
    :param q: SQL query.
    :return: Query with applied filters.
    """
    shout_slug = by.get("shout")
    if shout_slug:
        q = q.filter(Shout.slug == shout_slug)

    shouts = by.get("shouts")
    if shouts:
        q = q.filter(Shout.slug.in_(shouts))

    created_by = by.get("created_by")
    if created_by:
        q = q.filter(Author.id == created_by)

    author_slug = by.get("author")
    if author_slug:
        q = q.filter(Author.slug == author_slug)

    topic = by.get("topic")
    if isinstance(topic, int):
        q = q.filter(Shout.topics.any(id=topic))

    kinds = by.get("kinds")
    if isinstance(kinds, list):
        q = q.filter(Reaction.kind.in_(kinds))

    if by.get("reply_to"):
        q = q.filter(Reaction.reply_to == by.get("reply_to"))

    by_search = by.get("search", "")
    if len(by_search) > 2:
        q = q.filter(Reaction.body.ilike(f"%{by_search}%"))

    after = by.get("after")
    if isinstance(after, int):
        q = q.filter(Reaction.created_at > after)

    return q


@query.field("load_reactions_by")
async def load_reactions_by(_, info, by, limit=50, offset=0):
    """
    Load reactions based on specified parameters.

    :param info: GraphQL context info.
    :param by: Filter parameters.
    :param limit: Number of reactions to load.
    :param offset: Pagination offset.
    :return: List of reactions.
    """
    q = query_reactions()

    # Add statistics and apply filters
    q = add_reaction_stat_columns(q)
    q = apply_reaction_filters(by, q)
    q = q.where(Reaction.deleted_at.is_(None))

    # Group and sort
    q = q.group_by(Reaction.id, Author.id, Shout.id)
    order_stat = by.get("sort", "").lower()
    order_by_stmt = desc(Reaction.created_at)
    if order_stat == "oldest":
        order_by_stmt = asc(Reaction.created_at)
    elif order_stat.endswith("like"):
        order_by_stmt = desc("rating_stat")
    q = q.order_by(order_by_stmt)

    # Retrieve and return reactions
    return get_reactions_with_stat(q, limit, offset)


@query.field("load_shout_ratings")
async def load_shout_ratings(_, info, shout: int, limit=100, offset=0):
    """
    Load ratings for a specified shout with pagination.

    :param info: GraphQL context info.
    :param shout: Shout ID.
    :param limit: Number of reactions to load.
    :param offset: Pagination offset.
    :return: List of reactions.
    """
    q = query_reactions()

    # Filter, group, sort, limit, offset
    q = q.filter(
        and_(
            Reaction.deleted_at.is_(None),
            Reaction.shout == shout,
            Reaction.kind.in_(RATING_REACTIONS),
        )
    )
    q = q.group_by(Reaction.id, Author.id, Shout.id)
    q = q.order_by(desc(Reaction.created_at))

    # Retrieve and return reactions
    return get_reactions_with_stat(q, limit, offset)


@query.field("load_shout_comments")
async def load_shout_comments(_, info, shout: int, limit=50, offset=0):
    """
    Load comments for a specified shout with pagination and statistics.

    :param info: GraphQL context info.
    :param shout: Shout ID.
    :param limit: Number of comments to load.
    :param offset: Pagination offset.
    :return: List of reactions.
    """
    q = query_reactions()

    q = add_reaction_stat_columns(q)

    # Filter, group, sort, limit, offset
    q = q.filter(
        and_(
            Reaction.deleted_at.is_(None),
            Reaction.shout == shout,
            Reaction.body.is_not(None),
        )
    )
    q = q.group_by(Reaction.id, Author.id, Shout.id)
    q = q.order_by(desc(Reaction.created_at))

    # Retrieve and return reactions
    return get_reactions_with_stat(q, limit, offset)


@query.field("load_comment_ratings")
async def load_comment_ratings(_, info, comment: int, limit=50, offset=0):
    """
    Load ratings for a specified comment with pagination and statistics.

    :param info: GraphQL context info.
    :param comment: Comment ID.
    :param limit: Number of ratings to load.
    :param offset: Pagination offset.
    :return: List of reactions.
    """
    q = query_reactions()

    q = add_reaction_stat_columns(q)

    # Filter, group, sort, limit, offset
    q = q.filter(
        and_(
            Reaction.deleted_at.is_(None),
            Reaction.reply_to == comment,
            Reaction.kind == ReactionKind.COMMENT.value,
        )
    )
    q = q.group_by(Reaction.id, Author.id, Shout.id)
    q = q.order_by(desc(Reaction.created_at))

    # Retrieve and return reactions
    return get_reactions_with_stat(q, limit, offset)
