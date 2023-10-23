from datetime import datetime, timedelta, timezone
from sqlalchemy import and_, asc, desc, select, text, func, case
from sqlalchemy.orm import aliased
from services.presence import notify_reaction
from services.auth import login_required
from base.exceptions import OperationNotAllowed
from base.orm import local_session
from base.resolvers import mutation, query
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutReactionsFollower
from orm.author import Author


def add_reaction_stat_columns(q):
    aliased_reaction = aliased(Reaction)

    q = q.outerjoin(
        aliased_reaction, Reaction.id == aliased_reaction.replyTo
    ).add_columns(
        func.sum(aliased_reaction.id).label("reacted_stat"),
        func.sum(case((aliased_reaction.body.is_not(None), 1), else_=0)).label(
            "commented_stat"
        ),
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
                following = ShoutReactionsFollower.create(
                    follower=author_id, shout=shout.id, auto=auto
                )
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
        .filter(and_(Shout.publishedAt.is_not(None), Shout.deletedAt.is_(None)))
        .count()
        > 0
    )


def check_to_publish(session, author_id, reaction):
    """set shout to public if publicated approvers amount > 4"""
    if not reaction.replyTo and reaction.kind in [
        ReactionKind.ACCEPT,
        ReactionKind.LIKE,
        ReactionKind.PROOF,
    ]:
        if is_published_author(author_id):
            # now count how many approvers are voted already
            approvers_reactions = (
                session.query(Reaction).where(Reaction.shout == reaction.shout).all()
            )
            approvers = [
                author_id,
            ]
            for ar in approvers_reactions:
                a = ar.createdBy
                if is_published_author(session, a):
                    approvers.append(a)
            if len(approvers) > 4:
                return True
    return False


def check_to_hide(session, reaction):
    """hides any shout if 20% of reactions are negative"""
    if not reaction.replyTo and reaction.kind in [
        ReactionKind.REJECT,
        ReactionKind.DISLIKE,
        ReactionKind.DISPROOF,
    ]:
        # if is_published_author(author_id):
        approvers_reactions = (
            session.query(Reaction).where(Reaction.shout == reaction.shout).all()
        )
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
    s.publishedAt = datetime.now(tz=timezone.utc)
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
    author_id = info.context["author_id"]
    with local_session() as session:
        reaction["createdBy"] = author_id
        shout = session.query(Shout).where(Shout.id == reaction["shout"]).one()

        if reaction["kind"] in [ReactionKind.DISLIKE.name, ReactionKind.LIKE.name]:
            existing_reaction = (
                session.query(Reaction)
                .where(
                    and_(
                        Reaction.shout == reaction["shout"],
                        Reaction.createdBy == author_id,
                        Reaction.kind == reaction["kind"],
                        Reaction.replyTo == reaction.get("replyTo"),
                    )
                )
                .first()
            )

            if existing_reaction is not None:
                raise OperationNotAllowed("You can't vote twice")

            opposite_reaction_kind = (
                ReactionKind.DISLIKE
                if reaction["kind"] == ReactionKind.LIKE.name
                else ReactionKind.LIKE
            )
            opposite_reaction = (
                session.query(Reaction)
                .where(
                    and_(
                        Reaction.shout == reaction["shout"],
                        Reaction.createdBy == author_id,
                        Reaction.kind == opposite_reaction_kind,
                        Reaction.replyTo == reaction.get("replyTo"),
                    )
                )
                .first()
            )

            if opposite_reaction is not None:
                session.delete(opposite_reaction)

        r = Reaction.create(**reaction)

        # Proposal accepting logix
        if (
            r.replyTo is not None
            and r.kind == ReactionKind.ACCEPT
            and author_id in shout.dict()["authors"]
        ):
            replied_reaction = (
                session.query(Reaction).where(Reaction.id == r.replyTo).first()
            )
            if replied_reaction and replied_reaction.kind == ReactionKind.PROPOSE:
                if replied_reaction.range:
                    old_body = shout.body
                    start, end = replied_reaction.range.split(":")
                    start = int(start)
                    end = int(end)
                    new_body = old_body[:start] + replied_reaction.body + old_body[end:]
                    shout.body = new_body
                    # TODO: update git version control

        session.add(r)
        session.commit()
        rdict = r.dict()
        rdict["shout"] = shout.dict()
        author = session.query(Author).where(Author.id == author_id).first()
        rdict["createdBy"] = author.dict()

        # self-regulation mechanics

        if check_to_hide(session, r):
            set_hidden(session, r.shout)
        elif check_to_publish(session, author_id, r):
            set_published(session, r.shout)

        try:
            reactions_follow(author_id, reaction["shout"], True)
        except Exception as e:
            print(f"[resolvers.reactions] error on reactions auto following: {e}")

        rdict["stat"] = {"commented": 0, "reacted": 0, "rating": 0}

        # notification call
        notify_reaction(rdict)

        return {"reaction": rdict}


@mutation.field("updateReaction")
@login_required
async def update_reaction(_, info, rid, reaction={}):
    author_id = info.context["author_id"]
    with local_session() as session:
        q = select(Reaction).filter(Reaction.id == rid)
        q = add_reaction_stat_columns(q)
        q = q.group_by(Reaction.id)

        [r, reacted_stat, commented_stat, rating_stat] = (
            session.execute(q).unique().one()
        )

        if not r:
            return {"error": "invalid reaction id"}
        if r.createdBy != author_id:
            return {"error": "access denied"}

        r.body = reaction["body"]
        r.updatedAt = datetime.now(tz=timezone.utc)
        if r.kind != reaction["kind"]:
            # NOTE: change mind detection can be here
            pass
        if reaction.get("range"):
            r.range = reaction.get("range")
        session.commit()
        r.stat = {
            "commented": commented_stat,
            "reacted": reacted_stat,
            "rating": rating_stat,
        }

        notify_reaction(r.dict(), "update")

        return {"reaction": r}


@mutation.field("deleteReaction")
@login_required
async def delete_reaction(_, info, rid):
    author_id = info.context["author_id"]
    with local_session() as session:
        r = session.query(Reaction).filter(Reaction.id == rid).first()
        if not r:
            return {"error": "invalid reaction id"}
        if r.createdBy != author_id:
            return {"error": "access denied"}

        if r.kind in [ReactionKind.LIKE, ReactionKind.DISLIKE]:
            session.delete(r)
        else:
            r.deletedAt = datetime.now(tz=timezone.utc)
        session.commit()

        notify_reaction(r.dict(), "delete")

        return {"reaction": r}


@query.field("loadReactionsBy")
async def load_reactions_by(_, info, by, limit=50, offset=0):
    """
    :param info: graphql meta
    :param by: {
        :shout - filter by slug
        :shouts - filer by shout slug list
        :createdBy - to filter by author
        :topic - to filter by topic
        :search - to search by reactions' body
        :comment - true if body.length > 0
        :days - a number of days ago
        :sort - a fieldname to sort desc by default
    }
    :param limit: int amount of shouts
    :param offset: int offset in this order
    :return: Reaction[]
    """

    q = (
        select(Reaction, Author, Shout)
        .join(Author, Reaction.createdBy == Author.id)
        .join(Shout, Reaction.shout == Shout.id)
    )

    if by.get("shout"):
        q = q.filter(Shout.slug == by["shout"])
    elif by.get("shouts"):
        q = q.filter(Shout.slug.in_(by["shouts"]))

    if by.get("createdBy"):
        q = q.filter(Author.id == by.get("createdBy"))

    if by.get("topic"):
        # TODO: check
        q = q.filter(Shout.topics.contains(by["topic"]))

    if by.get("comment"):
        q = q.filter(func.length(Reaction.body) > 0)

    if len(by.get("search", "")) > 2:
        q = q.filter(Reaction.body.ilike(f'%{by["body"]}%'))

    if by.get("days"):
        after = datetime.now(tz=timezone.utc) - timedelta(days=int(by["days"]) or 30)
        q = q.filter(Reaction.createdAt > after)  # FIXME: use comparing operator?

    order_way = asc if by.get("sort", "").startswith("-") else desc
    order_field = by.get("sort", "").replace("-", "") or Reaction.createdAt
    q = q.group_by(Reaction.id, Author.id, Shout.id).order_by(order_way(order_field))
    q = add_reaction_stat_columns(q)
    q = q.where(Reaction.deletedAt.is_(None))
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
        reaction.createdBy = author
        reaction.shout = shout
        reaction.stat = {
            "rating": rating_stat,
            "commented": commented_stat,
            "reacted": reacted_stat,
        }
        reaction.kind = reaction.kind.name
        reactions.append(reaction)

    # ?
    if by.get("stat"):
        reactions.sort(lambda r: r.stat.get(by["stat"]) or r.createdAt)

    return reactions


@login_required
@query.field("followedReactions")
async def followed_reactions(_, info):
    author_id = info.context["author_id"]
    # FIXME: method should return array of shouts
    with local_session() as session:
        author = session.query(Author).where(Author.id == author_id).first()
        reactions = (
            session.query(Reaction.shout)
            .where(Reaction.createdBy == author.id)
            .filter(Reaction.createdAt > author.lastSeen)
            .all()
        )

        return reactions
