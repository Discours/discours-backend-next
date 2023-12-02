import time
from sqlalchemy.orm import joinedload, aliased
from sqlalchemy.sql.expression import desc, asc, select, func, case, and_, nulls_last

from services.auth import login_required
from services.db import local_session
from services.schema import query
from orm.author import AuthorFollower, Author
from orm.topic import TopicFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic, ShoutVisibility
from services.search import SearchService
from services.viewed import ViewedStorage


def add_stat_columns(q):
    aliased_reaction = aliased(Reaction)
    q = q.outerjoin(aliased_reaction).add_columns(
        func.sum(aliased_reaction.id).label("reacted_stat"),
        func.sum(
            case(
                (aliased_reaction.kind == ReactionKind.COMMENT.value, 1),
                else_=0
            )
        ).label("commented_stat"),
        func.sum(
            case(
                (aliased_reaction.kind == ReactionKind.AGREE.value, 1),
                (aliased_reaction.kind == ReactionKind.DISAGREE.value, -1),
                (aliased_reaction.kind == ReactionKind.PROOF.value, 1),
                (aliased_reaction.kind == ReactionKind.DISPROOF.value, -1),
                (aliased_reaction.kind == ReactionKind.ACCEPT.value, 1),
                (aliased_reaction.kind == ReactionKind.REJECT.value, -1),
                (aliased_reaction.kind == ReactionKind.LIKE.value, 1),
                (aliased_reaction.kind == ReactionKind.DISLIKE.value, -1),
                else_=0,
            )
        ).label("rating_stat"),
        func.max(
            case(
                (aliased_reaction.kind != ReactionKind.COMMENT.value, 0),
                else_=aliased_reaction.created_at,
            )
        ).label("last_comment"),
    )

    return q



def apply_filters(q, filters, author_id=None):
    # LoadShoutsFilters handling
    if filters.get("reacted") and author_id:
        q.join(Reaction, Reaction.created_by == author_id)

    by_published = filters.get("published")
    if by_published:
        q = q.filter(Shout.visibility == ShoutVisibility.PUBLIC.value)
    by_layouts = filters.get("layouts")
    if by_layouts:
        q = q.filter(Shout.layout.in_(by_layouts))
    by_author = filters.get("author")
    if by_author:
        q = q.filter(Shout.authors.any(slug=by_author))
    by_topic = filters.get("topic")
    if by_topic:
        q = q.filter(Shout.topics.any(slug=by_topic))
    by_after = filters.get("after")
    if by_after:
        ts = int(by_after)
        q = q.filter(Shout.created_at > ts)

    return q


@query.field("get_shout")
async def get_shout(_, _info, slug=None, shout_id=None):
    with local_session() as session:
        q = select(Shout).options(
            # joinedload(Shout.created_by),
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        q = add_stat_columns(q)

        if slug is not None:
            q = q.filter(Shout.slug == slug)

        if shout_id is not None:
            q = q.filter(Shout.id == shout_id)

        q = q.filter(Shout.deleted_at.is_(None)).group_by(Shout.id)

        try:
            author_stats = session.execute(q).first()
            if author_stats:
                [
                    shout,
                    viewed_stat,
                    reacted_stat,
                    commented_stat,
                    rating_stat,
                    _last_comment,
                ] = author_stats
                shout.stat = {
                    "viewed": viewed_stat,
                    "reacted": reacted_stat,
                    "commented": commented_stat,
                    "rating": rating_stat,
                }

                for author_caption in session.query(ShoutAuthor).join(Shout).where(Shout.slug == slug):
                    for author in shout.authors:
                        if author.id == author_caption.author:
                            author.caption = author_caption.caption
                return shout
        except Exception as e:
            import traceback

            traceback.print_exc()
            print(e)
            return None


@query.field("load_shouts_by")
async def load_shouts_by(_, info, options):
    """
    :param _:
    :param info:GraphQLInfo
    :param options: {
        filters: {
            layouts: ['audio', 'video', ..],
            reacted: True,
            published: True, // filter published-only
            author: 'discours',
            topic: 'culture',
            after: 1234567 // unixtime
        }
        offset: 0
        limit: 50
        order_by: 'created_at' | 'commented' | 'reacted' | 'rating'
        order_by_desc: true

    }
    :return: Shout[]
    """

    q = (
        select(Shout, Author)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .select_from(Shout)
        .where(Shout.deleted_at.is_(None))
    )

    # counters
    q = add_stat_columns(q)

    # filters
    q = apply_filters(q, options.get("filters", {}))

    # group
    q = q.group_by(Shout.id, Author.user, Author.name, Author.slug, Author.bio, Author.id)

    # order
    order_by = options.get("order_by", Shout.published_at)
    query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
    q = q.order_by(nulls_last(query_order_by))

    # limit offset
    offset = options.get("offset", 0)
    limit = options.get("limit", 10)
    q = q.limit(limit).offset(offset)

    shouts = []
    with local_session() as session:
        for [
            shout,
            reacted_stat,
            commented_stat,
            rating_stat,
            _last_comment,
        ] in session.execute(q).unique():
            shout.stat = {
                "viewed": ViewedStorage.get_shout(shout.slug),
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": rating_stat,
            }
            shouts.append(shout)

    return shouts


@login_required
@query.field("load_shouts_feed")
async def load_shouts_feed(_, info, options):
    user_id = info.context["user_id"]
    with local_session() as session:
        author = session.query(Author).filter(Author.user == user_id).first()
        if author:
            author_followed_authors = select(AuthorFollower.author).where(AuthorFollower.follower == author.id)
            author_followed_topics = select(TopicFollower.topic).where(TopicFollower.follower == author.id)

            subquery = (
                select(Shout.id)
                .where(Shout.id == ShoutAuthor.shout)
                .where(Shout.id == ShoutTopic.shout)
                .where(
                    (ShoutAuthor.author.in_(author_followed_authors)) | (ShoutTopic.topic.in_(author_followed_topics))
                )
            )

            q = (
                select(Shout)
                .options(
                    # joinedload(Shout.created_by, Author.id == Shout.created_by),
                    joinedload(Shout.authors),
                    joinedload(Shout.topics),
                )
                .where(
                    and_(
                        Shout.published_at.is_not(None),
                        Shout.deleted_at.is_(None),
                        Shout.id.in_(subquery),
                    )
                )
            )

            q = add_stat_columns(q)
            q = apply_filters(q, options.get("filters", {}), author.id)

            order_by = options.get("order_by", Shout.published_at)

            query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
            offset = options.get("offset", 0)
            limit = options.get("limit", 10)

            q = q.group_by(Shout.id).order_by(nulls_last(query_order_by)).limit(limit).offset(offset)

            shouts = []
            for [
                shout,
                reacted_stat,
                commented_stat,
                rating_stat,
                _last_comment,
            ] in session.execute(q).unique():
                shout.stat = {
                    "viewed": ViewedStorage.get_shout(shout.slug),
                    "reacted": reacted_stat,
                    "commented": commented_stat,
                    "rating": rating_stat,
                }
                shouts.append(shout)
            return shouts
    return []


@query.field("load_shouts_search")
async def load_shouts_search(_, _info, text, limit=50, offset=0):
    if text and len(text) > 2:
        return SearchService.search(text, limit, offset)
    else:
        return []
