import time
from typing import List
import logging

from sqlalchemy import and_, asc, case, desc, func, select, text, or_
from sqlalchemy.orm import aliased, joinedload
from sqlalchemy.sql import union

from orm.author import Author
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutReactionsFollower, ShoutVisibility
from services.auth import login_required, add_user_role
from services.db import local_session
from services.notify import notify_reaction
from services.schema import mutation, query
from services.viewed import ViewedStorage


logging.basicConfig()
logger = logging.getLogger("\t[resolvers.reaction]\t")
logger.setLevel(logging.DEBUG)

def add_stat_columns(q, aliased_reaction):

    q = q.outerjoin(aliased_reaction).add_columns(
        func.sum(case((aliased_reaction.kind == ReactionKind.COMMENT.value, 1), else_=0)).label("comments_stat"),
        func.sum(case((aliased_reaction.kind == ReactionKind.LIKE.value, 1), else_=0)).label("likes_stat"),
        func.sum(case((aliased_reaction.kind == ReactionKind.DISLIKE.value, 1), else_=0)).label("dislikes_stat"),
        func.max(case((aliased_reaction.kind != ReactionKind.COMMENT.value, None),else_=aliased_reaction.created_at)).label("last_comment"),
    )

    return q


def reactions_follow(author_id, shout_id, auto=False):
    try:
        with local_session() as session:
            shout = session.query(Shout).where(Shout.id == shout_id).one()

            following = (
                session.query(ShoutReactionsFollower)
                .where(
                    and_(
                        ShoutReactionsFollower.follower == author_id,
                        ShoutReactionsFollower.shout == shout.id,
                    )
                )
                .first()
            )

            if not following:
                following = ShoutReactionsFollower(follower=author_id, shout=shout.id, auto=auto)
                session.add(following)
                session.commit()
                return True
    except Exception:
        return False


def reactions_unfollow(author_id, shout_id: int):
    try:
        with local_session() as session:
            shout = session.query(Shout).where(Shout.id == shout_id).one()

            following = (
                session.query(ShoutReactionsFollower)
                .where(
                    and_(
                        ShoutReactionsFollower.follower == author_id,
                        ShoutReactionsFollower.shout == shout.id,
                    )
                )
                .first()
            )

            if following:
                session.delete(following)
                session.commit()
                return True
    except Exception:
        pass
    return False


def is_published_author(session, author_id):
    """checks if author has at least one publication"""
    return (
        session.query(Shout)
        .where(Shout.authors.any(id=author_id))
        .filter(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))
        .count()
        > 0
    )

def is_negative(x):
    return x in [
        ReactionKind.ACCEPT.value,
        ReactionKind.LIKE.value,
        ReactionKind.PROOF.value,
    ]

def is_positive(x):
    return x in [
        ReactionKind.ACCEPT.value,
        ReactionKind.LIKE.value,
        ReactionKind.PROOF.value,
    ]

def check_to_publish(session, approver_id, reaction):
    """set shout to public if publicated approvers amount > 4"""
    if not reaction.reply_to and is_positive(reaction.kind):
        if is_published_author(session, approver_id):
            # now count how many approvers are voted already
            approvers_reactions = session.query(Reaction).where(Reaction.shout == reaction.shout).all()
            approvers = [
                approver_id,
            ]
            for ar in approvers_reactions:
                a = ar.created_by
                if is_published_author(session, a):
                    approvers.append(a)
            if len(approvers) > 4:
                return True
    return False


def check_to_hide(session, reaction):
    """hides any shout if 20% of reactions are negative"""
    if not reaction.reply_to and is_negative(reaction.kind):
        # if is_published_author(author_id):
        approvers_reactions = session.query(Reaction).where(Reaction.shout == reaction.shout).all()
        rejects = 0
        for r in approvers_reactions:
            if is_negative(r.kind):
                rejects += 1
        if len(approvers_reactions) / rejects < 5:
            return True
    return False


async def set_published(session, shout_id, approver_id):
    s = session.query(Shout).where(Shout.id == shout_id).first()
    s.published_at = int(time.time())
    s.published_by = approver_id
    Shout.update(s, {"visibility": ShoutVisibility.PUBLIC.value})
    author = session.query(Author).filter(Author.id == s.created_by).first()
    if author:
        await add_user_role(str(author.user))
    session.add(s)
    session.commit()


def set_hidden(session, shout_id):
    s = session.query(Shout).where(Shout.id == shout_id).first()
    Shout.update(s, {"visibility": ShoutVisibility.COMMUNITY.value})
    session.add(s)
    session.commit()


async def _create_reaction(session, shout, author, reaction):
    r = Reaction(**reaction)
    rdict = r.dict()
    session.add(r)
    session.commit()

    # Proposal accepting logic
    if rdict.get("reply_to"):
        if r.kind in ["LIKE", "APPROVE"] and author.id in shout.authors:
            replied_reaction = session.query(Reaction).filter(Reaction.id == r.reply_to).first()
            if replied_reaction:
                if replied_reaction.kind is ReactionKind.PROPOSE.value:
                    if replied_reaction.range:
                        old_body = shout.body
                        start, end = replied_reaction.range.split(":")
                        start = int(start)
                        end = int(end)
                        new_body = old_body[:start] + replied_reaction.body + old_body[end:]
                        shout_dict = shout.dict()
                        shout_dict["body"] = new_body
                        Shout.update(shout, shout_dict)
                        session.add(shout)
                        session.commit()

    # Self-regulation mechanics
    if check_to_hide(session, r):
        set_hidden(session, shout.id)
    elif check_to_publish(session, author.id, r):
        await set_published(session, shout.id, author.id)

    # Reactions auto-following
    reactions_follow(author.id, reaction["shout"], True)

    rdict["shout"] = shout.dict()
    rdict["created_by"] = author.dict()
    rdict["stat"] = {"commented": 0, "reacted": 0, "rating": 0}

    # Notifications call
    await notify_reaction(rdict, "create")

    return rdict

@mutation.field("create_reaction")
@login_required
async def create_reaction(_, info, reaction):
    user_id = info.context["user_id"]

    shout_id = reaction.get("shout")

    if not shout_id:
        return {"error": "Shout ID is required to create a reaction."}

    try:
        with local_session() as session:
            shout = session.query(Shout).filter(Shout.id == shout_id).one()
            author = session.query(Author).filter(Author.user == user_id).first()
            if shout and author:
                reaction["created_by"] = author.id
                kind = reaction.get("kind")
                shout_id = shout.id

                if not kind and reaction.get("body"):
                    kind = ReactionKind.COMMENT.value

                if not kind:
                    return { "error": "cannot create reaction with this kind"}

                if kind in ["LIKE", "DISLIKE", "AGREE", "DISAGREE"]:
                    same_reaction = (
                        session.query(Reaction)
                        .filter(
                            and_(
                                Reaction.shout == shout_id,
                                Reaction.created_by == author.id,
                                Reaction.kind == kind,
                                Reaction.reply_to == reaction.get("reply_to"),
                            )
                        )
                        .first()
                    )

                    if same_reaction is not None:
                        return {"error": "You can't like or dislike same thing twice"}

                    opposite_reaction_kind = (
                        ReactionKind.DISLIKE.value
                        if reaction["kind"] == ReactionKind.LIKE.value
                        else ReactionKind.LIKE.value
                    )
                    opposite_reaction = (
                        session.query(Reaction)
                        .filter(
                            and_(
                                Reaction.shout == reaction["shout"],
                                Reaction.created_by == author.id,
                                Reaction.kind == opposite_reaction_kind,
                                Reaction.reply_to == reaction.get("reply_to"),
                            )
                        )
                        .first()
                    )

                    if opposite_reaction is not None:
                        return {"error": "Remove opposite vote first"}
                    else:
                        rdict = await _create_reaction(session, shout, author, reaction)
                        return {"reaction": rdict}
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"{type(e).__name__}: {e}")

    return {"error": "Cannot create reaction."}



@mutation.field("update_reaction")
@login_required
async def update_reaction(_, info, rid, reaction):
    user_id = info.context["user_id"]
    with local_session() as session:
        q = select(Reaction).filter(Reaction.id == rid)
        aliased_reaction = aliased(Reaction)
        q = add_stat_columns(q, aliased_reaction)
        q = q.group_by(Reaction.id)

        [r, commented_stat, likes_stat, dislikes_stat, _l] = session.execute(q).unique().one()

        if not r:
            return {"error": "invalid reaction id"}
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            if r.created_by != author.id:
                return {"error": "access denied"}
            body = reaction.get("body")
            if body:
                r.body = body
            r.updated_at = int(time.time())
            if r.kind != reaction["kind"]:
                # TODO: change mind detection can be here
                pass

            session.commit()
            r.stat = {
                "commented": commented_stat,
                "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
            }

            await notify_reaction(r.dict(), "update")

            return {"reaction": r}
        else:
            return {"error": "not authorized"}
    return {"error": "cannot create reaction"}

@mutation.field("delete_reaction")
@login_required
async def delete_reaction(_, info, rid):
    user_id = info.context["user_id"]
    with local_session() as session:
        r = session.query(Reaction).filter(Reaction.id == rid).first()
        if not r:
            return {"error": "invalid reaction id"}
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            if r.created_by is author.id:
                return {"error": "access denied"}

            if r.kind in [ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]:
                session.delete(r)
                session.commit()
                await notify_reaction(r.dict(), "delete")
        else:
            return {"error": "access denied"}
    return {}


def apply_reaction_filters(by, q):
    if by.get("shout"):
        q = q.filter(Shout.slug == by["shout"])

    elif by.get("shouts"):
        q = q.filter(Shout.slug.in_(by["shouts"]))

    if by.get("created_by"):
        q = q.filter(Author.id == by["created_by"])

    if by.get("topic"):
        q = q.filter(Shout.topics.contains(by["topic"]))

    if by.get("comment"):
        q = q.filter(func.length(Reaction.body) > 0)

    # NOTE: not using ElasticSearch here
    by_search = by.get("search", "")
    if len(by_search) > 2:
        q = q.filter(Reaction.body.ilike(f"%{by_search}%"))

    if by.get("after"):
        after = int(by["after"])
        q = q.filter(Reaction.created_at > after)

    return q


@query.field("load_reactions_by")
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
        .join(Author, Reaction.created_by == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    # calculate counters
    aliased_reaction = aliased(Reaction)
    q = add_stat_columns(q, aliased_reaction)

    # filter
    q = apply_reaction_filters(by, q)
    q = q.where(Reaction.deleted_at.is_(None))

    # group by
    q = q.group_by(Reaction.id, Author.id, Shout.id, aliased_reaction.id)

    # order by
    q = q.order_by(desc("created_at"))

    # pagination
    q = q.limit(limit).offset(offset)

    reactions = []
    with local_session() as session:
        result_rows = session.execute(q)
        for [
            reaction,
            author,
            shout,
            commented_stat,
            likes_stat,
            dislikes_stat,
            _last_comment
        ] in result_rows:
            reaction.created_by = author
            reaction.shout = shout
            reaction.stat = {
                "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                "commented": commented_stat
                }
            reactions.append(reaction)

        # sort if by stat is present
        stat_sort = by.get("stat")
        if stat_sort:
            reactions = sorted(reactions, key=lambda r: r.stat.get(stat_sort) or r.created_at, reverse=stat_sort.startswith("-"))

    return reactions



async def reacted_shouts_updates(follower_id: int, limit=50, offset=0) -> List[Shout]:
    shouts: List[Shout] = []
    with local_session() as session:
        author = session.query(Author).filter(Author.id == follower_id).first()
        if author:
            # Shouts where follower is the author
            q1 = select(Shout).outerjoin(
                Reaction, and_(Reaction.shout_id == Shout.id, Reaction.created_by == follower_id)
            ).outerjoin(
                Author, Shout.authors.any(id=follower_id)
            ).options(
                joinedload(Shout.reactions),
                joinedload(Shout.authors)
            )
            q1 = add_stat_columns(q1, aliased(Reaction))
            q1 = q1.filter(Author.id == follower_id).group_by(Shout.id)

            # Shouts where follower reacted
            q2 = (
                select(Shout)
                .join(Reaction, Reaction.shout_id == Shout.id)
                .options(
                    joinedload(Shout.reactions),
                    joinedload(Shout.authors)
                )
                .filter(Reaction.created_by == follower_id)
                .group_by(Shout.id)
            )
            q2 = add_stat_columns(q2, aliased(Reaction))

            # Sort shouts by the `last_comment` field
            combined_query = union(q1, q2).order_by(desc("last_comment")).limit(limit).offset(offset)
            results = session.execute(combined_query).scalars()
            with local_session() as session:
                for [
                    shout,
                    commented_stat,
                    likes_stat,
                    dislikes_stat,
                    last_comment
                ] in results:
                    shout.stat = {
                        "viewed": await ViewedStorage.get_shout(shout.slug),
                        "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                        "commented": commented_stat,
                        "last_comment": last_comment
                        }
                    shouts.append(shout)

    return shouts

@query.field("load_shouts_followed")
@login_required
async def load_shouts_followed(_, info, limit=50, offset=0) -> List[Shout]:
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            try:
                author_id: int = author.dict()["id"]
                shouts = await reacted_shouts_updates(author_id, limit, offset)
                return shouts
            except Exception as error:
                logger.debug(error)
    return []
