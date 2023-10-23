from datetime import datetime, timedelta, timezone

from aiohttp.web_exceptions import HTTPException
from sqlalchemy.orm import joinedload, aliased
from sqlalchemy.sql.expression import desc, asc, select, func, case, and_, nulls_last

from services.auth import login_required
from services.db import local_session
from orm.topic import TopicFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.author import AuthorFollower


def add_stat_columns(q):
    aliased_reaction = aliased(Reaction)

    q = q.outerjoin(aliased_reaction).add_columns(
        func.sum(aliased_reaction.id).label("reacted_stat"),
        func.sum(
            case((aliased_reaction.kind == ReactionKind.COMMENT, 1), else_=0)
        ).label("commented_stat"),
        func.sum(
            case(
                # do not count comments' reactions
                (aliased_reaction.replyTo.is_not(None), 0),
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
        func.max(
            case(
                (aliased_reaction.kind != ReactionKind.COMMENT, None),
                else_=aliased_reaction.createdAt,
            )
        ).label("last_comment"),
    )

    return q


def apply_filters(q, filters, author_id=None):
    if filters.get("reacted") and author_id:
        q.join(Reaction, Reaction.createdBy == author_id)

    v = filters.get("visibility")
    if v == "public":
        q = q.filter(Shout.visibility == filters.get("visibility"))
    if v == "community":
        q = q.filter(Shout.visibility.in_(["public", "community"]))

    if filters.get("layout"):
        q = q.filter(Shout.layout == filters.get("layout"))
    if filters.get("author"):
        q = q.filter(Shout.authors.any(slug=filters.get("author")))
    if filters.get("topic"):
        q = q.filter(Shout.topics.any(slug=filters.get("topic")))
    if filters.get("title"):
        q = q.filter(Shout.title.ilike(f'%{filters.get("title")}%'))
    if filters.get("body"):
        q = q.filter(Shout.body.ilike(f'%{filters.get("body")}%s'))
    if filters.get("days"):
        before = datetime.now(tz=timezone.utc) - timedelta(
            days=int(filters.get("days")) or 30
        )
        q = q.filter(Shout.createdAt > before)

    return q


async def load_shout(_, _info, slug=None, shout_id=None):
    with local_session() as session:
        q = select(Shout).options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        q = add_stat_columns(q)

        if slug is not None:
            q = q.filter(Shout.slug == slug)

        if shout_id is not None:
            q = q.filter(Shout.id == shout_id)

        q = q.filter(Shout.deletedAt.is_(None)).group_by(Shout.id)

        try:
            [
                shout,
                reacted_stat,
                commented_stat,
                rating_stat,
                _last_comment,
            ] = session.execute(q).first()

            shout.stat = {
                "viewed": shout.views,
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": rating_stat,
            }

            for author_caption in (
                session.query(ShoutAuthor).join(Shout).where(Shout.slug == slug)
            ):
                for author in shout.authors:
                    if author.id == author_caption.author:
                        author.caption = author_caption.caption
            return shout
        except Exception:
            raise HTTPException(status_code=404, detail="Slug was not found: %s" % slug)


async def load_shouts_by(_, info, options):
    """
    :param _:
    :param info:GraphQLInfo
    :param options: {
        filters: {
            layout: 'audio',
            visibility: "public",
            author: 'discours',
            topic: 'culture',
            title: 'something',
            body: 'something else',
            days: 30
        }
        offset: 0
        limit: 50
        order_by: 'createdAt' | 'commented' | 'reacted' | 'rating'
        order_by_desc: true

    }
    :return: Shout[]
    """

    q = (
        select(Shout)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .where(Shout.deletedAt.is_(None))
    )

    q = add_stat_columns(q)

    author_id = info.context["author_id"]
    q = apply_filters(q, options.get("filters", {}), author_id)

    order_by = options.get("order_by", Shout.publishedAt)

    query_order_by = (
        desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
    )
    offset = options.get("offset", 0)
    limit = options.get("limit", 10)

    q = (
        q.group_by(Shout.id)
        .order_by(nulls_last(query_order_by))
        .limit(limit)
        .offset(offset)
    )

    shouts = []
    shouts_map = {}
    with local_session() as session:
        for [
            shout,
            reacted_stat,
            commented_stat,
            rating_stat,
            _last_comment,
        ] in session.execute(q).unique():
            shouts.append(shout)
            shout.stat = {
                "viewed": shout.views,
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": rating_stat,
            }
            shouts_map[shout.id] = shout

    return shouts


@login_required
async def get_my_feed(_, info, options):
    author_id = info.context["author_id"]
    with local_session() as session:
        subquery = (
            select(Shout.id)
            .join(ShoutAuthor)
            .join(AuthorFollower, AuthorFollower.follower._is(author_id))
            .join(ShoutTopic)
            .join(TopicFollower, TopicFollower.follower._is(author_id))
        )

        q = (
            select(Shout)
            .options(
                joinedload(Shout.authors),
                joinedload(Shout.topics),
            )
            .where(
                and_(
                    Shout.publishedAt.is_not(None),
                    Shout.deletedAt.is_(None),
                    Shout.id.in_(subquery),
                )
            )
        )

        q = add_stat_columns(q)
        q = apply_filters(q, options.get("filters", {}), author_id)

        order_by = options.get("order_by", Shout.publishedAt)

        query_order_by = (
            desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
        )
        offset = options.get("offset", 0)
        limit = options.get("limit", 10)

        q = (
            q.group_by(Shout.id)
            .order_by(nulls_last(query_order_by))
            .limit(limit)
            .offset(offset)
        )

        shouts = []
        shouts_map = {}
        for [
            shout,
            reacted_stat,
            commented_stat,
            rating_stat,
            _last_comment,
        ] in session.execute(q).unique():
            shouts.append(shout)
            shout.stat = {
                "viewed": shout.views,
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": rating_stat,
            }
            shouts_map[shout.id] = shout
    # FIXME: shouts_map does not go anywhere?
    return shouts
