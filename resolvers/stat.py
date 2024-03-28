import json

from sqlalchemy import func, distinct, select, join, and_, case, true
from sqlalchemy.orm import aliased

from orm.reaction import Reaction, ReactionKind
from orm.topic import TopicFollower, Topic
from services.db import local_session
from orm.author import AuthorFollower, Author, AuthorRating
from orm.shout import ShoutTopic, ShoutAuthor, Shout
from services.logger import root_logger as logger
from services.rediscache import redis


def add_topic_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_topic_follower = aliased(TopicFollower)
    aliased_shout_topic = aliased(ShoutTopic)

    q = (
        q.outerjoin(aliased_shout_topic, aliased_shout_topic.topic == Topic.id)
        .add_columns(
            func.count(distinct(aliased_shout_topic.shout)).label('shouts_stat')
        )
        .outerjoin(
            aliased_shout_author,
            aliased_shout_topic.shout == aliased_shout_author.shout,
        )
        .add_columns(
            func.count(distinct(aliased_shout_author.author)).label('authors_stat')
        )
        .outerjoin(aliased_topic_follower)
        .add_columns(
            func.count(distinct(aliased_topic_follower.follower)).label(
                'followers_stat'
            )
        )
    )
    # Create a subquery for comments count
    _sub_comments = (
        select(
            Shout.id, func.coalesce(func.count(Reaction.id), 0).label('comments_count')
        )
        .join(
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

    # q = q.outerjoin(sub_comments, aliased_shout_topic.shout == sub_comments.c.id)
    # q = q.add_columns(
    #    func.coalesce(func.sum(sub_comments.c.comments_count), 0).label('comments_stat')
    # )

    q = q.group_by(Topic.id)

    return q


def add_author_stat_columns(q, with_rating=False):
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
            Author.id,
            func.coalesce(func.count(Reaction.id)).label('comments_count')
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

    q = q.outerjoin(AuthorRating, AuthorRating.author == Author.id)
    q = q.add_columns(
        func.sum(case((AuthorRating.plus == true(), 1), else_=0)).label('likes_count'),
        func.sum(case((AuthorRating.plus != true(), 1), else_=0)).label('dislikes_count'),
    )

    if with_rating:
        subq = select(Reaction).where(
            and_(
                Reaction.shout == Shout.id,
                Shout.authors.any(id=Author.id),
                Reaction.reply_to.is_(None),
                Reaction.deleted_at.is_(None)
            )
        ).subquery()

        q = q.outerjoin(subq, subq.c.shout == Shout.id)
        q = q.add_columns(
            func.count(case((subq.c.kind == ReactionKind.LIKE.value, subq.c.id), else_=None)).label('shouts_likes'),
            func.count(case((subq.c.kind == ReactionKind.DISLIKE.value, subq.c.id), else_=None)).label('shouts_dislikes'),
        )

    q = q.group_by(
        Author.id,
        sub_comments.c.comments_count
    )

    return q


def get_with_stat(q, with_rating=False):
    try:
        is_author = f'{q}'.lower().startswith('select author')
        is_topic = f'{q}'.lower().startswith('select topic')
        if is_author:
            q = add_author_stat_columns(q, with_rating)
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
                    if with_rating:
                        logger.debug('author, shouts, authors, followers, comments, author_likes, author_dislikes, shouts_likes, shouts_dislikes, comment_likes, comments_dislikes')
                        logger.debug(cols)
                        stat['rating'] = cols[6] - cols[7]
                        stat['rating_shouts'] = cols[8] - cols[9]
                entity.stat = stat
                records.append(entity)
    except Exception as exc:
        import traceback

        traceback.print_exc()
        raise Exception(exc)
    return records


async def get_authors_with_stat_cached(q):
    # logger.debug(q)
    try:
        records = []
        with local_session() as session:
            for [x] in session.execute(q):
                stat_str = await redis.execute('GET', f'author:{x.id}')
                x.stat = json.loads(stat_str).get('stat') if isinstance(stat_str, str) else {}
                records.append(x)
    except Exception as exc:
        raise Exception(exc)
    return records


async def get_topics_with_stat_cached(q):
    try:
        records = []
        current = None
        with local_session() as session:
            for [x] in session.execute(q):
                current = x
                stat_str = await redis.execute('GET', f'topic:{x.id}')
                if isinstance(stat_str, str):
                    x.stat = json.loads(stat_str).get('stat')
                records.append(x)
    except Exception as exc:
        logger.error(current)
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
