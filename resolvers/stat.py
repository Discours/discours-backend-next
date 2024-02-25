from sqlalchemy import func, distinct, select, join
from sqlalchemy.orm import aliased

from orm.topic import TopicFollower, Topic
from resolvers.rating import load_author_ratings
from services.db import local_session
from orm.author import AuthorFollower, Author
from orm.shout import ShoutTopic, ShoutAuthor
from services.logger import root_logger as logger


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

    q = q.group_by(Topic.id)

    return q


def add_author_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_author_authors = aliased(AuthorFollower)
    aliased_author_followers = aliased(AuthorFollower)

    q = (
        q.outerjoin(aliased_shout_author, aliased_shout_author.author == Author.id)
        .add_columns(
            func.count(distinct(aliased_shout_author.shout)).label('shouts_stat')
        )
        .outerjoin(aliased_author_authors, aliased_author_authors.follower == Author.id)
        .add_columns(
            func.count(distinct(aliased_shout_author.author)).label('authors_stat')
        )
        .outerjoin(
            aliased_author_followers, aliased_author_followers.author == Author.id
        )
        .add_columns(
            func.count(distinct(aliased_author_followers.follower)).label(
                'followers_stat'
            )
        )
    )

    q = q.group_by(Author.id)

    return q


def get_with_stat(q):
    q = add_author_stat_columns(q)
    records = []
    logger.debug(q.replace('\n', ' '))
    with local_session() as session:
        for [entity, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            entity.stat = {
                'shouts': shouts_stat,
                'authors': authors_stat,
                'followers': followers_stat,
            }
            if q.startswith('SELECT author'):
                load_author_ratings(session, entity)
            records.append(entity)

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
