from sqlalchemy import and_, distinct, func, join, select
from sqlalchemy.orm import aliased

from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.db import local_session
from services.cache import cache_author


def add_topic_stat_columns(q):
    aliased_shout_topic = aliased(ShoutTopic)
    aliased_authors = aliased(ShoutAuthor)
    aliased_followers = aliased(TopicFollower)
    aliased_shout = aliased(Shout)

    q = q.outerjoin(aliased_shout_topic, aliased_shout_topic.topic == Topic.id)
    q = q.add_columns(
        func.count(distinct(aliased_shout_topic.shout)).label('shouts_stat')
    )

    q = q.outerjoin(aliased_shout, and_(
        aliased_shout.id == aliased_shout_topic.shout,
        aliased_shout.published_at.is_not(None),
        aliased_shout.deleted_at.is_(None)
    ))
    q = q.outerjoin(aliased_authors, aliased_shout.authors.contains(aliased_authors.author))
    q = q.add_columns(
        func.count(distinct(aliased_authors.author)).label('authors_stat')
    )

    q = q.outerjoin(aliased_followers, aliased_followers.topic == Topic.id)
    q = q.add_columns(
        func.count(distinct(aliased_followers.follower)).label('followers_stat')
    )

    # Create a subquery for comments count
    sub_comments = (
        select(
            Shout.id.label('shout_id'),
            func.coalesce(func.count(Reaction.id)).label('comments_count')
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


    q = q.outerjoin(sub_comments, aliased_shout_topic.shout == sub_comments.c.shout_id)
    q = q.add_columns(func.coalesce(sub_comments.c.comments_count, 0).label('comments_stat'))

    group_list = [Topic.id, sub_comments.c.comments_count]

    q = q.group_by(*group_list)

    return q


def add_author_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_authors = aliased(AuthorFollower)
    aliased_followers = aliased(AuthorFollower)

    q = q.outerjoin(aliased_shout_author, aliased_shout_author.author == Author.id)
    q = q.add_columns(
        func.count(distinct(aliased_shout_author.shout)).label('shouts_stat')
    )

    q = q.outerjoin(aliased_authors, aliased_authors.follower == Author.id)
    q = q.add_columns(
        func.count(distinct(aliased_authors.author)).label('authors_stat')
    )

    q = q.outerjoin(aliased_followers, aliased_followers.author == Author.id)
    q = q.add_columns(
        func.count(distinct(aliased_followers.follower)).label('followers_stat')
    )

    # Create a subquery for comments count
    sub_comments = (
        select(
            Author.id, func.coalesce(func.count(Reaction.id)).label('comments_count')
        )
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

    q = q.outerjoin(sub_comments, Author.id == sub_comments.c.id)
    q = q.add_columns(sub_comments.c.comments_count)
    group_list = [Topic.id, sub_comments.c.comments_count]

    q = q.group_by(*group_list)

    return q


def get_with_stat(q):
    try:
        is_author = f'{q}'.lower().startswith('select author')
        is_topic = f'{q}'.lower().startswith('select topic')
        if is_author:
            q = add_author_stat_columns(q)
        elif is_topic:
            q = add_topic_stat_columns(q)
        records = []
        with local_session() as session:
            result = session.execute(q)
            for cols in result:
                entity = cols[0]
                stat = dict()
                stat['shouts'] = cols[1]
                stat['authors'] = cols[2]
                stat['followers'] = cols[3]
                if is_author:
                    stat['comments'] = cols[4]
                entity.stat = stat
                records.append(entity)
    except Exception as exc:
        import traceback

        traceback.print_exc()
        raise Exception(exc)
    return records


def author_follows_authors(author_id: int):
    af = aliased(AuthorFollower, name='af')
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


async def update_author_stat(author: Author):
    author_with_stat = get_with_stat(select(Author).where(Author=author.id))
    if isinstance(author_with_stat, Author):
        author_dict = author_with_stat.dict()
        await cache_author(author_dict)
