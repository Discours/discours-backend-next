from sqlalchemy import func, distinct
from sqlalchemy.orm import aliased

from orm.author import Author, AuthorFollower
from orm.shout import ShoutAuthor, ShoutTopic
from orm.topic import Topic, TopicFollower
from services.db import local_session
# from services.viewed import ViewedStorage


def add_author_stat_columns(q, author_model=None):
    aliased_author = author_model or aliased(Author)
    shout_author_aliased = aliased(ShoutAuthor)
    q = q.outerjoin(shout_author_aliased).add_columns(
        func.count(distinct(shout_author_aliased.shout)).label('shouts_stat')
    )

    authors_table = aliased(AuthorFollower)
    q = q.outerjoin(
        authors_table, authors_table.follower == aliased_author.id
    ).add_columns(func.count(distinct(authors_table.author)).label('authors_stat'))

    followers_table = aliased(AuthorFollower)
    q = q.outerjoin(followers_table, followers_table.author == aliased_author.id).add_columns(
        func.count(distinct(followers_table.follower)).label('followers_stat')
    )

    q = q.group_by(aliased_author.id)
    return q


async def get_authors_from_query(q):
    authors = []
    with local_session() as session:
        for [author, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            author.stat = {
                'shouts': shouts_stat,
                'followers': followers_stat,
                'followings': authors_stat,
                # viewed
            }
            authors.append(author)
    return authors


def add_topic_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_topic_follower = aliased(TopicFollower)

    q = (
        q.outerjoin(ShoutTopic, Topic.id == ShoutTopic.topic)
        .add_columns(func.count(distinct(ShoutTopic.shout)).label('shouts_stat'))
        .outerjoin(aliased_shout_author, ShoutTopic.shout == aliased_shout_author.shout)
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


async def get_topics_from_query(q):
    topics = []
    with local_session() as session:
        for [topic, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            topic.stat = {
                'shouts': shouts_stat,
                'authors': authors_stat,
                'followers': followers_stat,
                # 'viewed': await ViewedStorage.get_topic(topic.slug),
            }
            topics.append(topic)

    return topics
