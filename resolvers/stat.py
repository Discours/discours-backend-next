from sqlalchemy import func, distinct
from sqlalchemy.orm import aliased

from orm.topic import TopicFollower, Topic
from services.db import local_session
from orm.author import AuthorFollower, Author
from orm.shout import ShoutTopic, ShoutAuthor


def add_topic_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_topic_follower = aliased(TopicFollower)

    q = (
        q.outerjoin(ShoutTopic, Topic.id == ShoutTopic.topic)
        .add_columns(func.count(distinct(ShoutTopic.shout)).label('shouts_stat'))
        .outerjoin(aliased_shout_author, ShoutTopic.shout == aliased_shout_author.shout)
        .add_columns(func.count(distinct(aliased_shout_author.author)).label('authors_stat'))
        .outerjoin(aliased_topic_follower, aliased_topic_follower.topic == Topic.id)
        .add_columns(func.count(distinct(aliased_topic_follower.follower)).label('followers_stat'))
    )

    q = q.group_by(Topic.id)

    return q


def add_author_stat_columns(q):
    aliased_author_followers = aliased(AuthorFollower)
    aliased_author_authors = aliased(AuthorFollower)
    q = (
        q.outerjoin(ShoutAuthor, Author.id == ShoutAuthor.author)
        .add_columns(func.count(distinct(ShoutAuthor.shout)).label('shouts_stat'))
        .outerjoin(aliased_author_authors, AuthorFollower.follower == Author.id)
        .add_columns(func.count(distinct(aliased_author_authors.author)).label('authors_stat'))
        .outerjoin(aliased_author_followers, AuthorFollower.author == Author.id)
        .add_columns(func.count(distinct(aliased_author_followers.follower)).label('followers_stat'))
    )

    q = q.group_by(Author.id)

    return q


def unpack_stat(q):
    records = []
    with local_session() as session:
        for [entity, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            entity.stat = {
                'shouts': shouts_stat,
                'authors': authors_stat,
                'followers': followers_stat
            }
            records.append(entity)

    return records


def get_authors_with_stat(q):
    q = add_author_stat_columns(q)
    return unpack_stat(q)


def get_topics_with_stat(q):
    q = add_topic_stat_columns(q)
    return unpack_stat(q)
