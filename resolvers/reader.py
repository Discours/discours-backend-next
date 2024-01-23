from sqlalchemy import bindparam, distinct, or_, literal
from sqlalchemy.orm import aliased, joinedload
from sqlalchemy.sql.expression import and_, asc, case, desc, func, nulls_last, select
from starlette.exceptions import HTTPException

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic, ShoutVisibility
from orm.topic import Topic, TopicFollower
from services.auth import login_required
from services.db import local_session
from services.schema import query
from services.search import SearchService
from services.viewed import ViewedStorage
from resolvers.topic import get_random_topic
from resolvers.reaction import add_stat_columns


def apply_filters(q, filters, author_id=None):  # noqa: C901
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
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        aliased_reaction = aliased(Reaction)
        q = add_stat_columns(q, aliased_reaction)

        if slug is not None:
            q = q.filter(Shout.slug == slug)

        if shout_id is not None:
            q = q.filter(Shout.id == shout_id)

        q = q.filter(Shout.deleted_at.is_(None)).group_by(Shout.id)

        try:
            results = session.execute(q).first()
            if results:
                [shout, commented_stat, likes_stat, dislikes_stat, _last_comment] = results

                shout.stat = {
                    "viewed": await ViewedStorage.get_shout(shout.slug),
                    # "reacted": reacted_stat,
                    "commented": commented_stat,
                    "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
                }

                for author_caption in session.query(ShoutAuthor).join(Shout).where(Shout.slug == slug):
                    for author in shout.authors:
                        if author.id == author_caption.author:
                            author.caption = author_caption.caption
                main_topic = (
                    session.query(Topic.slug)
                    .join(
                        ShoutTopic,
                        and_(ShoutTopic.topic == Topic.id, ShoutTopic.shout == shout.id, ShoutTopic.main == True),
                    )
                    .first()
                )

                if main_topic:
                    shout.main_topic = main_topic[0]
                return shout
        except Exception:
            raise HTTPException(status_code=404, detail=f"shout {slug or shout_id} not found")


@query.field("load_shouts_by")
async def load_shouts_by(_, _info, options):
    """
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
        order_by: 'created_at' | 'commented'  | 'likes_stat'
        order_by_desc: true

    }
    :return: Shout[]
    """

    # base
    q = (
        select(Shout)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .where(and_(Shout.deleted_at.is_(None), Shout.layout.is_not(None)))
    )

    # stats
    aliased_reaction = aliased(Reaction)
    q = add_stat_columns(q, aliased_reaction)

    # filters
    q = apply_filters(q, options.get("filters", {}))

    # group
    q = q.group_by(Shout.id)

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
        for [shout, commented_stat, likes_stat, dislikes_stat, _last_comment] in session.execute(q).unique():
            main_topic = (
                session.query(Topic.slug)
                .join(
                    ShoutTopic,
                    and_(
                        ShoutTopic.topic == Topic.id, ShoutTopic.shout == shout.id, ShoutTopic.main == True
                    ),  # noqa: E712
                )
                .first()
            )

            if main_topic:
                shout.main_topic = main_topic[0]
            shout.stat = {
                "viewed": await ViewedStorage.get_shout(shout.slug),
                "commented": commented_stat,
                "rating": int(likes_stat) - int(dislikes_stat),
            }
            shouts.append(shout)

    return shouts


@query.field("load_shouts_drafts")
@login_required
async def load_shouts_drafts(_, info):
    user_id = info.context["user_id"]

    q = (
        select(Shout)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .filter(Shout.deleted_at.is_(None))
        .filter(Shout.visibility == ShoutVisibility.AUTHORS.value)
    )

    shouts = []
    with local_session() as session:
        reader = session.query(Author).filter(Author.user == user_id).first()
        if reader:
            q = q.filter(Shout.created_by == reader.id)
            q = q.group_by(Shout.id)
            for [shout] in session.execute(q).unique():
                main_topic = (
                    session.query(Topic.slug)
                    .join(
                        ShoutTopic,
                        and_(ShoutTopic.topic == Topic.id, ShoutTopic.shout == shout.id, ShoutTopic.main == True),
                    )
                    .first()
                )

                if main_topic:
                    shout.main_topic = main_topic[0]
                shouts.append(shout)

    return shouts


@query.field("load_shouts_feed")
@login_required
async def load_shouts_feed(_, info, options):
    user_id = info.context["user_id"]

    shouts = []
    with local_session() as session:
        reader = session.query(Author).filter(Author.user == user_id).first()
        if reader:
            reader_followed_authors = select(AuthorFollower.author).where(AuthorFollower.follower == reader.id)
            reader_followed_topics = select(TopicFollower.topic).where(TopicFollower.follower == reader.id)

            subquery = (
                select(Shout.id)
                .where(Shout.id == ShoutAuthor.shout)
                .where(Shout.id == ShoutTopic.shout)
                .where((ShoutAuthor.author.in_(reader_followed_authors)) | (ShoutTopic.topic.in_(reader_followed_topics)))
            )

            q = (
                select(Shout)
                .options(
                    joinedload(Shout.authors),
                    joinedload(Shout.topics),
                )
                .where(and_(Shout.published_at.is_not(None), Shout.deleted_at.is_(None), Shout.id.in_(subquery)))
            )

            aliased_reaction = aliased(Reaction)
            q = add_stat_columns(q, aliased_reaction)
            q = apply_filters(q, options.get("filters", {}), reader.id)

            order_by = options.get("order_by", Shout.published_at)

            query_order_by = desc(order_by) if options.get("order_by_desc", True) else asc(order_by)
            offset = options.get("offset", 0)
            limit = options.get("limit", 10)

            q = q.group_by(Shout.id).order_by(nulls_last(query_order_by)).limit(limit).offset(offset)

            # print(q.compile(compile_kwargs={"literal_binds": True}))

            for [shout, reacted_stat, commented_stat, _last_comment] in session.execute(q).unique():
                main_topic = (
                    session.query(Topic.slug)
                    .join(
                        ShoutTopic,
                        and_(ShoutTopic.topic == Topic.id, ShoutTopic.shout == shout.id, ShoutTopic.main == True),
                    )
                    .first()
                )

                if main_topic:
                    shout.main_topic = main_topic[0]
                shout.stat = {
                    "viewed": await ViewedStorage.get_shout(shout.slug),
                    "reacted": reacted_stat,
                    "commented": commented_stat,
                }
                shouts.append(shout)

    return shouts


@query.field("load_shouts_search")
async def load_shouts_search(_, _info, text, limit=50, offset=0):
    if text and len(text) > 2:
        results = await SearchService.search(text, limit, offset)
        results_dict = {r["slug"]: r for r in results}
        # print(results_dict)

        q = (
            select(Shout)
            .options(
                joinedload(Shout.authors),
                joinedload(Shout.topics),
            )
            .where(and_(Shout.deleted_at.is_(None), Shout.slug.in_(results_dict.keys())))
        )

        shouts_data = []
        with local_session() as session:
            results = list(session.execute(q).unique())
            # print(results)
            print(f"[resolvers.reader] searched, preparing {len(results)} results")
            for x in results:
                shout = x[0]
                shout_slug = shout.dict().get("slug", "")
                score = results_dict.get(shout_slug, {}).get("score", 0)
                shout_data = shout.dict()  # Convert the Shout instance to a dictionary
                shout_data["score"] = score  # Add the score to the dictionary
                shouts_data.append(shout_data)

        return shouts_data
    else:
        return []


@login_required
@query.field("load_shouts_unrated")
async def load_shouts_unrated(_, info, limit: int = 50, offset: int = 0):
    q = (
        select(Shout)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .outerjoin(
            Reaction,
            and_(
                Reaction.shout == Shout.id,
                Reaction.replyTo.is_(None),
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
    q = add_stat_columns(q, aliased_reaction)

    q = q.group_by(Shout.id).order_by(func.random()).limit(limit).offset(offset)
    user_id = info.context.get("user_id")
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
        for [shout,commented_stat, likes_stat, dislikes_stat, last_comment] in session.execute(
            q, {"author_id": author_id}
        ).unique():
            shouts.append(shout)
            shout.stat = {
                "viewed": await ViewedStorage.get_shout(shout_slug=shout.slug),
                "commented": commented_stat,
                "rating": int(likes_stat or 0) - int(dislikes_stat or 0),
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

    subquery = select(Shout.id).outerjoin(aliased_reaction).where(Shout.deleted_at.is_(None))

    subquery = apply_filters(subquery, options.get("filters", {}))
    subquery = subquery.group_by(Shout.id).order_by(desc(
            func.sum(
                case(
                    (Reaction.kind == ReactionKind.LIKE.value, 1),
                    (Reaction.kind == ReactionKind.AGREE.value, 1),
                    (Reaction.kind == ReactionKind.DISLIKE.value, -1),
                    (Reaction.kind == ReactionKind.DISAGREE.value, -1),
                    else_=0
                )
            )
        )
    )

    random_limit = options.get("random_limit")
    if random_limit:
        subquery = subquery.limit(random_limit)

    q = (
        select(Shout)
        .options(
            joinedload(Shout.authors),
            joinedload(Shout.topics),
        )
        .where(Shout.id.in_(subquery))
    )
    aliased_reaction = aliased(Reaction)
    q = add_stat_columns(q, aliased_reaction)

    limit = options.get("limit", 10)
    q = q.group_by(Shout.id).order_by(func.random()).limit(limit)

    # print(q.compile(compile_kwargs={"literal_binds": True}))

    return await get_shouts_from_query(q)


@query.field("load_shouts_random_topic")
async def load_shouts_random_topic(_, info, limit: int = 10):
    topic = get_random_topic()
    shouts = []
    if topic:
        q = (
            select(Shout)
            .options(
                joinedload(Shout.authors),
                joinedload(Shout.topics),
            )
            .filter(and_(Shout.deleted_at.is_(None), Shout.visibility == ShoutVisibility.PUBLIC.value, Shout.topics.any(slug=topic.slug)))
        )

        aliased_reaction = aliased(Reaction)
        q = add_stat_columns(q, aliased_reaction)

        q = q.group_by(Shout.id).order_by(desc(Shout.created_at)).limit(limit)

        shouts = get_shouts_from_query(q)

    return {"topic": topic, "shouts": shouts}
