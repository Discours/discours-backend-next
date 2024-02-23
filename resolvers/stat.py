from sqlalchemy import func, distinct
from sqlalchemy.orm import aliased

from orm.topic import TopicFollower, Topic
from services.db import local_session
from orm.author import AuthorFollower, Author
from orm.shout import ShoutTopic, ShoutAuthor


def add_topic_stat_columns(q):
    aliased_shout_authors = aliased(ShoutAuthor)
    aliased_topic_followers = aliased(TopicFollower)
    aliased_topic = aliased(Topic)
    q = (
        q.outerjoin(ShoutTopic, aliased_topic.id == ShoutTopic.topic)
        .with_entities(func.count(distinct(ShoutTopic.shout)).label('shouts_stat'))
        .outerjoin(aliased_shout_authors, ShoutTopic.shout == aliased_shout_authors.shout)
        .with_entities(func.count(distinct(aliased_shout_authors.author)).label('authors_stat'))
        .outerjoin(aliased_topic_followers, aliased_topic.id == aliased_topic_followers.topic)
        .with_entities(func.count(distinct(aliased_topic_followers.follower)).label('followers_stat'))
    )

    q = q.group_by(aliased_topic.id)

    return q


def add_author_stat_columns(q):
    aliased_author_authors = aliased(AuthorFollower, name='af_authors')
    aliased_author_followers = aliased(AuthorFollower, name='af_followers')
    aliased_author = aliased(Author)
    q = (
        q.outerjoin(ShoutAuthor, aliased_author.id == ShoutAuthor.author)
        .with_entities(func.count(distinct(ShoutAuthor.shout)).label('shouts_stat'))
        .outerjoin(aliased_author_authors, AuthorFollower.follower == aliased_author.id)
        .with_entities(func.count(distinct(aliased_author_authors.author)).label('authors_stat'))
        .outerjoin(aliased_author_followers, AuthorFollower.author == aliased_author.id)
        .with_entities(func.count(distinct(aliased_author_followers.follower)).label('followers_stat'))
    )

    q = q.group_by(aliased_author.id)

    return q


def execute_with_ministat(q):
    records = []
    with local_session() as session:
        for [r, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            r.stat = {
                'shouts': shouts_stat,
                'authors': authors_stat,
                'followers': followers_stat
            }
            records.append(r)

    return records


def get_authors_with_stat(q):
    q = add_author_stat_columns(q)
    return execute_with_ministat(q)


def get_topics_with_stat(q):
    q = add_topic_stat_columns(q)
    return execute_with_ministat(q)
