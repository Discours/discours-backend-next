from sqlalchemy import bindparam, distinct, or_, text
from sqlalchemy.orm import aliased, joinedload
from sqlalchemy.sql.expression import and_, asc, case, desc, func, nulls_last, select

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from resolvers.reaction import add_reaction_stat_columns
from resolvers.topic import get_topics_random
from services.auth import login_required
from services.db import local_session
from services.logger import root_logger as logger
from services.schema import query
from services.search import search_text
from services.viewed import ViewedStorage


def query_shouts():
    return (
        select(Shout)
        .options(joinedload(Shout.authors), joinedload(Shout.topics))
        .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None)))

    )


def filter_my(info, session, q):
    user_id = info.context.get("user_id")
    reader_id = info.context.get("author", {}).get("id")
    if user_id and reader_id:
        reader_followed_authors = select(AuthorFollower.author).where(AuthorFollower.follower == reader_id)
        reader_followed_topics = select(TopicFollower.topic).where(TopicFollower.follower == reader_id)

        subquery = (
            select(Shout.id)
            .where(Shout.id == ShoutAuthor.shout)
            .where(Shout.id == ShoutTopic.shout)
            .where((ShoutAuthor.author.in_(reader_followed_authors)) | (ShoutTopic.topic.in_(reader_followed_topics)))
        )
        q = q.filter(Shout.id.in_(subquery))
    return q, reader_id


def apply_filters(q, filters, author_id=None):
    if isinstance(filters, dict):
        if filters.get("reacted"):
            q = q.join(
                Reaction,
                and_(
                    Reaction.shout == Shout.id,
                    Reaction.created_by == author_id,
                ),
            )

        featured_filter = filters.get("featured", "")
        if featured_filter:
            q = q.filter(Shout.featured_at.is_not(None))
        elif "featured" in filters:
            q = q.filter(Shout.featured_at.is_(None))
        else:
            pass
        by_layouts = filters.get("layouts")
        if by_layouts and isinstance(by_layouts, list):
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
async def get_shout(_, info, slug: str):
    try:
        with local_session() as session:
            q = query_shouts()
            aliased_reaction = aliased(Reaction)
            q = add_reaction_stat_columns(q, aliased_reaction)
            q = q.filter(Shout.slug == slug)
            q = q.group_by(Shout.id)

            results = session.execute(q).first()
            if results:
                [
                    shout,
                    reacted_stat,
                    commented_stat,
                    likes_stat,
                    dislikes_stat,
                    last_comment,
                ] = results

                shout.stat = {
                    "viewed": await ViewedStorage.get_shout(shout.slug),
                    "reacted": reacted_stat,
                    "commented": commented_stat,
                    "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                    "last_comment": last_comment,
                }

                for author_caption in (
                    session.query(ShoutAuthor)
                    .join(Shout)
                    .where(
                        and_(
                            Shout.slug == slug,
                            Shout.published_at.is_not(None),
                            Shout.deleted_at.is_(None),
                        )
                    )
                ):
                    for author in shout.authors:
                        if author.id == author_caption.author:
                            author.caption = author_caption.caption
                main_topic = (
                    session.query(Topic.slug)
                    .join(
                        ShoutTopic,
                        and_(
                            ShoutTopic.topic == Topic.id,
                            ShoutTopic.shout == shout.id,
                            ShoutTopic.main.is_(True),
                        ),
                    )
                    .first()
                )

                if main_topic:
                    shout.main_topic = main_topic[0]
                return shout
    except Exception as _exc:
        import traceback

        logger.error(traceback.format_exc())


@query.field("load_shouts_by")
async def load_shouts_by(_, _info, options):
    """
    :param options: {
        filters: {
            layouts: ['audio', 'video', ..],
            reacted: True,
            featured: True, // filter featured-only
            author: 'discours',
            topic: 'culture',
            after: 1234567 // unixtime
        }
        offset: 0
        limit: 50
        order_by: 'created_at' | 'commented'  | 'likes_stat'
        order_by_desc: true

    }
    :return: Shout[]
    """

    # base
    q = query_shouts()

    # stats
    aliased_reaction = aliased(Reaction)
    q = add_reaction_stat_columns(q, aliased_reaction)

    # filters
    filters = options.get("filters", {})
    q = apply_filters(q, filters)

    # group
    q = q.group_by(Shout.id)

    # order
    order_by = Shout.featured_at if filters.get("featured") else Shout.published_at
    order_str = options.get("order_by")
    if order_str in ["likes", "followers", "comments", "last_comment"]:
        q = q.order_by(desc(text(f"{order_str}_stat")))
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
            likes_stat,
            dislikes_stat,
            last_comment,
        ] in session.execute(q).unique():
            main_topic = (
                session.query(Topic.slug)
                .join(
                    ShoutTopic,
                    and_(
                        ShoutTopic.topic == Topic.id,
                        ShoutTopic.shout == shout.id,
                        ShoutTopic.main.is_(True),
                    ),
                )
                .first()
            )

            if main_topic:
                shout.main_topic = main_topic[0]
            shout.stat = {
                "viewed": await ViewedStorage.get_shout(shout.slug),
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": int(likes_stat) - int(dislikes_stat),
                "last_comment": last_comment,
            }
            shouts.append(shout)

    return shouts


@query.field("load_shouts_feed")
@login_required
async def load_shouts_feed(_, info, options):
    shouts = []
    with local_session() as session:
        q = query_shouts()

        aliased_reaction = aliased(Reaction)
        q = add_reaction_stat_columns(q, aliased_reaction)

        # filters
        filters = options.get("filters", {})
        if filters:
            q, reader_id = filter_my(info, session, q)
            q = apply_filters(q, filters, reader_id)

        # sort order
        order_by = options.get(
            "order_by",
            Shout.featured_at if filters.get("featured") else Shout.published_at,
        )

        query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)

        # pagination
        offset = options.get("offset", 0)
        limit = options.get("limit", 10)

        q = q.group_by(Shout.id).order_by(nulls_last(query_order_by)).limit(limit).offset(offset)

        logger.debug(q.compile(compile_kwargs={"literal_binds": True}))

        for [
            shout,
            reacted_stat,
            commented_stat,
            likes_stat,
            dislikes_stat,
            last_comment,
        ] in session.execute(q).unique():
            main_topic = (
                session.query(Topic.slug)
                .join(
                    ShoutTopic,
                    and_(
                        ShoutTopic.topic == Topic.id,
                        ShoutTopic.shout == shout.id,
                        ShoutTopic.main.is_(True),
                    ),
                )
                .first()
            )

            if main_topic:
                shout.main_topic = main_topic[0]
            shout.stat = {
                "viewed": await ViewedStorage.get_shout(shout.slug),
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": likes_stat - dislikes_stat,
                "last_comment": last_comment,
            }
            shouts.append(shout)

    return shouts


@query.field("load_shouts_search")
async def load_shouts_search(_, _info, text, limit=50, offset=0):
    if isinstance(text, str) and len(text) > 2:
        results = await search_text(text, limit, offset)
        scores = {}
        hits_ids = []
        for sr in results:
            shout_id = sr.get("id")
            if shout_id:
                shout_id = str(shout_id)
                scores[shout_id] = sr.get("score")
                hits_ids.append(shout_id)
        result = local_session().execute(query_shouts()).unique()
        if result:
            logger.debug(result)
            shouts = []
            for shout in result.first():
                logger.debug(shout)
                shout.score = scores[f"{shout.id}"]
                shouts.append(shout)
            shouts.sort(key=lambda x: x.score, reverse=True)
        return shouts
    return []


@query.field("load_shouts_unrated")
@login_required
async def load_shouts_unrated(_, info, limit: int = 50, offset: int = 0):
    q = query_shouts()
    q = (
        q.outerjoin(
            Reaction,
            and_(
                Reaction.shout == Shout.id,
                Reaction.reply_to.is_(None),
                Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value]),
            ),
        )
        .outerjoin(Author, Author.user == bindparam("user_id"))
        .where(
            and_(
                Shout.deleted_at.is_(None),
                Shout.layout.is_not(None),
                or_(Author.id.is_(None), Reaction.created_by != Author.id),
            )
        )
    )

    # 3 or fewer votes is 0, 1, 2 or 3 votes (null, reaction id1, reaction id2, reaction id3)
    q = q.having(func.count(distinct(Reaction.id)) <= 4)

    aliased_reaction = aliased(Reaction)
    q = add_reaction_stat_columns(q, aliased_reaction)

    q = q.group_by(Shout.id).order_by(func.random()).limit(limit).offset(offset)
    user_id = info.context.get("user_id") if isinstance(info.context, dict) else None
    if user_id:
        with local_session() as session:
            author = session.query(Author).filter(Author.user == user_id).first()
            if author:
                return await get_shouts_from_query(q, author.id)
    else:
        return await get_shouts_from_query(q)


async def get_shouts_from_query(q, author_id=None):
    shouts = []
    with local_session() as session:
        for [
            shout,
            reacted_stat,
            commented_stat,
            likes_stat,
            dislikes_stat,
            last_comment,
        ] in session.execute(q, {"author_id": author_id}).unique():
            shouts.append(shout)
            shout.stat = {
                "viewed": await ViewedStorage.get_shout(shout_slug=shout.slug),
                "reacted": reacted_stat,
                "commented": commented_stat,
                "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                "last_comment": last_comment,
            }

    return shouts


@query.field("load_shouts_random_top")
async def load_shouts_random_top(_, _info, options):
    """
    :param _
    :param _info: GraphQLInfoContext
    :param options: {
        filters: {
            layouts: ['music']
            after: 13245678
        }
        random_limit: 100
        limit: 50
        offset: 0
    }
    :return: Shout[]
    """

    aliased_reaction = aliased(Reaction)

    subquery = (
        select(Shout.id).outerjoin(aliased_reaction).where(and_(Shout.deleted_at.is_(None), Shout.layout.is_not(None)))
    )

    subquery = apply_filters(subquery, options.get("filters", {}))

    subquery = subquery.group_by(Shout.id).order_by(
        desc(
            func.sum(
                case(
                    # do not count comments' reactions
                    (aliased_reaction.reply_to.is_not(None), 0),
                    (aliased_reaction.kind == ReactionKind.LIKE.value, 1),
                    (aliased_reaction.kind == ReactionKind.DISLIKE.value, -1),
                    else_=0,
                )
            )
        )
    )

    random_limit = options.get("random_limit", 100)
    if random_limit:
        subquery = subquery.limit(random_limit)

    q = select(Shout).options(joinedload(Shout.authors), joinedload(Shout.topics)).where(Shout.id.in_(subquery))

    q = add_reaction_stat_columns(q, aliased_reaction)

    limit = options.get("limit", 10)
    q = q.group_by(Shout.id).order_by(func.random()).limit(limit)

    shouts = await get_shouts_from_query(q)

    return shouts


@query.field("load_shouts_random_topic")
async def load_shouts_random_topic(_, info, limit: int = 10):
    [topic] = get_topics_random(None, None, 1)
    if topic:
        shouts = fetch_shouts_by_topic(topic, limit)
        if shouts:
            return {"topic": topic, "shouts": shouts}
    return {
        "error": "failed to get random topic after few retries",
        "shouts": [],
        "topic": {},
    }


def fetch_shouts_by_topic(topic, limit):
    q = (
        select(Shout)
        .options(joinedload(Shout.authors), joinedload(Shout.topics))
        .filter(
            and_(
                Shout.deleted_at.is_(None),
                Shout.featured_at.is_not(None),
                Shout.topics.any(slug=topic.slug),
            )
        )
    )

    aliased_reaction = aliased(Reaction)
    q = add_reaction_stat_columns(q, aliased_reaction)

    q = q.group_by(Shout.id).order_by(desc(Shout.created_at)).limit(limit)

    shouts = get_shouts_from_query(q)

    return shouts
