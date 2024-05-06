from sqlalchemy import and_, distinct, func, join, select
from sqlalchemy.orm import aliased

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.cache import cache_author
from services.db import local_session
from services.logger import root_logger as logger


def add_topic_stat_columns(q):
    aliased_shout = aliased(ShoutTopic)
    q = q.outerjoin(aliased_shout).add_columns(
        func.count(distinct(aliased_shout.shout)).label("shouts_stat")
    )
    aliased_follower = aliased(TopicFollower)
    q = q.outerjoin(aliased_follower, aliased_follower.topic == Topic.id).add_columns(
        func.count(distinct(aliased_follower.follower)).label("followers_stat")
    )

    q = q.group_by(Topic.id)

    return q


def add_author_stat_columns(q):
    aliased_shout = aliased(ShoutAuthor)
    q = q.outerjoin(aliased_shout).add_columns(
        func.count(distinct(aliased_shout.shout)).label("shouts_stat")
    )
    aliased_follower = aliased(AuthorFollower)
    q = q.outerjoin(
        aliased_follower, aliased_follower.follower == Author.id
    ).add_columns(
        func.count(distinct(aliased_follower.follower)).label("followers_stat")
    )

    q = q.group_by(Author.id)

    return q


def get_topic_shouts_stat(topic_id: int):
    q = (
        select(func.count(distinct(ShoutTopic.shout)))
        .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
        .filter(
            and_(
                ShoutTopic.topic == topic_id,
                Shout.published_at.is_not(None),
                Shout.deleted_at.is_(None),
            )
        )
    )
    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_topic_authors_stat(topic_id: int):
    # authors
    q = (
        select(func.count(distinct(ShoutAuthor.author)))
        .select_from(join(ShoutTopic, Shout, ShoutTopic.shout == Shout.id))
        .join(ShoutAuthor, ShoutAuthor.shout == Shout.id)
        .filter(
            and_(
                ShoutTopic.topic == topic_id,
                Shout.published_at.is_not(None),
                Shout.deleted_at.is_(None),
            )
        )
    )
    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_topic_followers_stat(topic_id: int):
    aliased_followers = aliased(TopicFollower)
    q = select(func.count(distinct(aliased_followers.follower))).filter(
        aliased_followers.topic == topic_id
    )
    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_topic_comments_stat(topic_id: int):
    sub_comments = (
        select(
            Shout.id.label("shout_id"),
            func.coalesce(func.count(Reaction.id)).label("comments_count"),
        )
        .join(ShoutTopic, ShoutTopic.shout == Shout.id)
        .join(Topic, ShoutTopic.topic == Topic.id)
        .outerjoin(
            Reaction,
            and_(
                Reaction.shout == Shout.id,
                Reaction.kind == ReactionKind.COMMENT.value,
                Reaction.deleted_at.is_(None),
            ),
        )
        .group_by(Shout.id)
        .subquery()
    )
    q = select(func.coalesce(func.sum(sub_comments.c.comments_count), 0)).filter(
        ShoutTopic.topic == topic_id
    )
    q = q.outerjoin(sub_comments, ShoutTopic.shout == sub_comments.c.shout_id)

    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_author_shouts_stat(author_id: int):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_shout = aliased(Shout)

    q = (
        select(func.count(distinct(aliased_shout.id)))
        .select_from(aliased_shout)
        .join(aliased_shout_author, aliased_shout.id == aliased_shout_author.shout)
        .filter(
            and_(
                aliased_shout_author.author == author_id,
                aliased_shout.published_at.is_not(None),
            )
        )
    )

    with local_session() as session:
        result = session.execute(q).first()

    return result[0] if result else 0


def get_author_authors_stat(author_id: int):
    aliased_authors = aliased(AuthorFollower)
    q = select(func.count(distinct(aliased_authors.author))).filter(
        and_(
            aliased_authors.follower == author_id,
            aliased_authors.author != author_id,
        )
    )
    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_author_followers_stat(author_id: int):
    aliased_followers = aliased(AuthorFollower)
    q = select(func.count(distinct(aliased_followers.follower))).filter(
        aliased_followers.author == author_id
    )
    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_author_comments_stat(author_id: int):
    sub_comments = (
        select(
            Author.id, func.coalesce(func.count(Reaction.id)).label("comments_count")
        )
        .select_from(Author)  # явно указываем левый элемент join'а
        .outerjoin(
            Reaction,
            and_(
                Reaction.created_by == Author.id,
                Reaction.kind == ReactionKind.COMMENT.value,
                Reaction.deleted_at.is_(None),
            ),
        )
        .group_by(Author.id)
        .subquery()
    )
    q = select(sub_comments.c.comments_count).filter(sub_comments.c.id == author_id)

    result = local_session().execute(q).first()
    return result[0] if result else 0


def get_with_stat(q):
    records = []
    try:
        is_author = f"{q}".lower().startswith("select author")
        # is_topic = f"{q}".lower().startswith("select topic")
        result = []
        if is_author:
            q = add_author_stat_columns(q)
        else:
            q = add_topic_stat_columns(q)
        with local_session() as session:
            result = session.execute(q)

            for cols in result:
                entity = cols[0]
                stat = dict()
                stat["shouts"] = cols[1]
                stat["followers"] = cols[2]
                if is_author:
                    stat["authors"] = get_author_authors_stat(entity.id)
                    stat["comments"] = get_author_comments_stat(entity.id)
                else:
                    stat["authors"] = get_topic_authors_stat(entity.id)
                entity.stat = stat
                records.append(entity)
    except Exception as exc:
        import traceback

        traceback.print_exc()
        logger.error(exc, exc_info=True)
    return records


def author_follows_authors(author_id: int):
    af = aliased(AuthorFollower, name="af")
    q = (
        select(Author)
        .select_from(join(Author, af, Author.id == af.author))
        .where(af.follower == author_id)
    )
    return get_with_stat(q)


def author_follows_topics(author_id: int):
    q = (
        select(Topic)
        .select_from(join(Topic, TopicFollower, Topic.id == TopicFollower.topic))
        .where(TopicFollower.follower == author_id)
    )
    return get_with_stat(q)


async def update_author_stat(author_id: int):
    author_with_stat = get_with_stat(select(Author).where(Author.id == author_id))
    if isinstance(author_with_stat, Author):
        author_dict = author_with_stat.dict()
        await cache_author(author_dict)
