import time
from typing import List

from sqlalchemy import and_, asc, desc, select, text, func, case
from sqlalchemy.orm import aliased
from services.notify import notify_reaction
from services.auth import login_required
from services.db import local_session
from services.schema import mutation, query
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutReactionsFollower
from orm.author import Author


def add_reaction_stat_columns(q):
    aliased_reaction = aliased(Reaction)

    q = q.outerjoin(aliased_reaction, Reaction.id == aliased_reaction.reply_to).add_columns(
        func.sum(aliased_reaction.id).label("reacted_stat"),
        func.sum(case((aliased_reaction.body.is_not(''), 1), else_=0)).label("commented_stat"),
        func.sum(
            case(
                (aliased_reaction.kind == ReactionKind.AGREE, 1),
                (aliased_reaction.kind == ReactionKind.DISAGREE, -1),
                (aliased_reaction.kind == ReactionKind.PROOF, 1),
                (aliased_reaction.kind == ReactionKind.DISPROOF, -1),
                (aliased_reaction.kind == ReactionKind.ACCEPT, 1),
                (aliased_reaction.kind == ReactionKind.REJECT, -1),
                (aliased_reaction.kind == ReactionKind.LIKE, 1),
                (aliased_reaction.kind == ReactionKind.DISLIKE, -1),
                else_=0,
            )
        ).label("rating_stat"),
    )

    return q


def reactions_follow(author_id, shout_id: int, auto=False):
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


def reactions_unfollow(author_id: int, shout_id: int):
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
        .where(Shout.authors.contains(author_id))
        .filter(and_(Shout.published_at.is_not(''), Shout.deleted_at.is_(None)))
        .count()
        > 0
    )


def check_to_publish(session, author_id, reaction):
    """set shout to public if publicated approvers amount > 4"""
    if not reaction.reply_to and reaction.kind in [
        ReactionKind.ACCEPT,
        ReactionKind.LIKE,
        ReactionKind.PROOF,
    ]:
        if is_published_author(session, author_id):
            # now count how many approvers are voted already
            approvers_reactions = session.query(Reaction).where(Reaction.shout == reaction.shout).all()
            approvers = [
                author_id,
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
    if not reaction.reply_to and reaction.kind in [
        ReactionKind.REJECT,
        ReactionKind.DISLIKE,
        ReactionKind.DISPROOF,
    ]:
        # if is_published_author(author_id):
        approvers_reactions = session.query(Reaction).where(Reaction.shout == reaction.shout).all()
        rejects = 0
        for r in approvers_reactions:
            if r.kind in [
                ReactionKind.REJECT,
                ReactionKind.DISLIKE,
                ReactionKind.DISPROOF,
            ]:
                rejects += 1
        if len(approvers_reactions) / rejects < 5:
            return True
    return False


def set_published(session, shout_id):
    s = session.query(Shout).where(Shout.id == shout_id).first()
    s.published_at = int(time.time())
    s.visibility = text("public")
    session.add(s)
    session.commit()


def set_hidden(session, shout_id):
    s = session.query(Shout).where(Shout.id == shout_id).first()
    s.visibility = text("community")
    session.add(s)
    session.commit()


@mutation.field("createReaction")
@login_required
async def create_reaction(_, info, reaction):
    user_id = info.context["user_id"]
    with local_session() as session:
        shout = session.query(Shout).where(Shout.id == reaction["shout"]).one()
        author = session.query(Author).where(Author.user == user_id).first()
        if shout and author:
            reaction["created_by"] = author.id
            if reaction["kind"] in [ReactionKind.DISLIKE.name, ReactionKind.LIKE.name]:
                existing_reaction = (
                    session.query(Reaction)
                    .where(
                        and_(
                            Reaction.shout == reaction["shout"],
                            Reaction.created_by == author.id,
                            Reaction.kind == reaction["kind"],
                            Reaction.reply_to == reaction.get("reply_to"),
                        )
                    )
                    .first()
                )

                if existing_reaction is not None:
                    return {"error": "You can't vote twice"}

                opposite_reaction_kind = (
                    ReactionKind.DISLIKE if reaction["kind"] == ReactionKind.LIKE.name else ReactionKind.LIKE
                )
                opposite_reaction = (
                    session.query(Reaction)
                    .where(
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
                    session.delete(opposite_reaction)

            r = Reaction(**reaction)

            # Proposal accepting logix
            if r.reply_to is not None and r.kind == ReactionKind.ACCEPT and author.id in shout.dict()["authors"]:
                replied_reaction = session.query(Reaction).where(Reaction.id == r.reply_to).first()
                if replied_reaction and replied_reaction.kind == ReactionKind.PROPOSE:
                    if replied_reaction.range:
                        old_body = shout.body
                        start, end = replied_reaction.range.split(":")
                        start = int(start)
                        end = int(end)
                        new_body = old_body[:start] + replied_reaction.body + old_body[end:]
                        shout.body = new_body

            session.add(r)
            session.commit()
            rdict = r.dict()
            rdict["shout"] = shout.dict()
            rdict["created_by"] = author.dict()

            # self-regulation mechanics

            if check_to_hide(session, r):
                set_hidden(session, r.shout)
            elif check_to_publish(session, author.id, r):
                set_published(session, r.shout)

            try:
                reactions_follow(author.id, reaction["shout"], True)
            except Exception as e:
                print(f"[resolvers.reactions] error on reactions auto following: {e}")

            rdict["stat"] = {"commented": 0, "reacted": 0, "rating": 0}

            # notifications call
            await notify_reaction(rdict, "create")

            return {"reaction": rdict}


@mutation.field("updateReaction")
@login_required
async def update_reaction(_, info, rid, reaction):
    user_id = info.context["user_id"]
    with local_session() as session:
        q = select(Reaction).filter(Reaction.id == rid)
        q = add_reaction_stat_columns(q)
        q = q.group_by(Reaction.id)

        [r, reacted_stat, commented_stat, rating_stat] = session.execute(q).unique().one()

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
                # NOTE: change mind detection can be here
                pass

            # FIXME: range is not stable after body editing
            if reaction.get("range"):
                r.range = reaction.get("range")

            session.commit()
            r.stat = {
                "commented": commented_stat,
                "reacted": reacted_stat,
                "rating": rating_stat,
            }

            await notify_reaction(r.dict(), "update")

            return {"reaction": r}
        else:
            return {"error": "user"}


@mutation.field("deleteReaction")
@login_required
async def delete_reaction(_, info, rid):
    user_id = info.context["user_id"]
    with local_session() as session:
        r = session.query(Reaction).filter(Reaction.id == rid).first()
        if not r:
            return {"error": "invalid reaction id"}
        author = session.query(Author).filter(Author.user == user_id).first()
        if not author or r.created_by != author.id:
            return {"error": "access denied"}

        if r.kind in [ReactionKind.LIKE, ReactionKind.DISLIKE]:
            session.delete(r)
        else:
            r.deleted_at = int(time.time())
        session.commit()

        await notify_reaction(r.dict(), "delete")

        return {"reaction": r}


@query.field("loadReactionsBy")
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
        :time_ago - amount of time ago
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

    if by.get("shout"):
        q = q.filter(Shout.slug == by["shout"])
    elif by.get("shouts"):
        q = q.filter(Shout.slug.in_(by["shouts"]))

    if by.get("created_by"):
        q = q.filter(Author.id == by.get("created_by"))

    if by.get("topic"):
        # TODO: check
        q = q.filter(Shout.topics.contains(by["topic"]))

    if by.get("comment"):
        q = q.filter(func.length(Reaction.body) > 0)

    if len(by.get("search", "")) > 2:
        q = q.filter(Reaction.body.ilike(f'%{by["body"]}%'))

    if by.get("time_ago"):
        after = int(time.time()) - int(by.get("time_ago", 0))
        q = q.filter(Reaction.created_at > after)

    order_way = asc if by.get("sort", "").startswith("-") else desc
    order_field = by.get("sort", "").replace("-", "") or Reaction.created_at
    q = q.group_by(Reaction.id, Author.id, Shout.id).order_by(order_way(order_field))
    q = add_reaction_stat_columns(q)
    q = q.where(Reaction.deleted_at.is_(None))
    q = q.limit(limit).offset(offset)
    reactions = []
    session = info.context["session"]
    for [
        reaction,
        author,
        shout,
        reacted_stat,
        commented_stat,
        rating_stat,
    ] in session.execute(q):
        reaction.created_by = author
        reaction.shout = shout
        reaction.stat = {
            "rating": rating_stat,
            "commented": commented_stat,
            "reacted": reacted_stat,
        }
        reaction.kind = reaction.kind.name
        reactions.append(reaction)

    # sort if by stat is present
    if by.get("stat"):
        reactions = sorted(reactions, key=lambda r: r.stat.get(by["stat"]) or r.created_at)

    return reactions


def reacted_shouts_updates(follower_id):
    shouts = []
    with local_session() as session:
        author = session.query(Author).where(Author.id == follower_id).first()
        if author:
            shouts = (
                session.query(Reaction.shout)
                .join(Shout)
                .filter(Reaction.created_by == author.id)
                .filter(Reaction.created_at > author.last_seen)
                .all()
            )
    return shouts


@login_required
@query.field("followedReactions")
async def get_reacted_shouts(_, info) -> List[Shout]:
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            shouts = reacted_shouts_updates(author.id)
            return shouts
        else:
            return []
