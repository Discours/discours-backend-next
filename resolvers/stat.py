from sqlalchemy import func, select, distinct, alias
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
        .add_columns(func.count(distinct(ShoutTopic.shout)).label('shouts_stat'))
        .outerjoin(
            aliased_shout_authors, ShoutTopic.shout == aliased_shout_authors.shout
        )
        .add_columns(
            func.count(distinct(aliased_shout_authors.author)).label('authors_stat')
        )
        .outerjoin(
            aliased_topic_followers, aliased_topic.id == aliased_topic_followers.topic
        )
        .add_columns(
            func.count(distinct(aliased_topic_followers.follower)).label(
                'followers_stat'
            )
        )
    )

    q = q.group_by(aliased_topic.id)

    return q


def add_author_stat_columns(q):
    aliased_author_authors = aliased(AuthorFollower, name='af_authors')
    aliased_author_followers = aliased(
        AuthorFollower, name='af_followers'
    )  # Добавлен второй псевдоним
    aliased_author = aliased(Author)
    q = (
        q.outerjoin(ShoutAuthor, aliased_author.id == ShoutAuthor.author)
        .add_columns(func.count(distinct(ShoutAuthor.shout)).label('shouts_stat'))
        .outerjoin(
            aliased_author_authors, aliased_author_authors.follower == aliased_author.id
        )
        .add_columns(
            func.count(distinct(aliased_author_authors.author)).label('authors_stat')
        )
        .outerjoin(
            aliased_author_followers,
            aliased_author_followers.author == aliased_author.id,
        )  # Используется второй псевдоним
        .add_columns(
            func.count(distinct(aliased_author_followers.follower)).label(
                'followers_stat'
            )
        )  # Используется второй псевдоним
    )

    q = q.group_by(aliased_author.id)

    return q


def execute_with_ministat(q):
    records = []
    with local_session() as session:
        for [entity, shouts_stat, authors_stat, followers_stat] in session.execute(q):
            entity.stat = {
                'shouts': shouts_stat,
                'authors': authors_stat,
                'followers': followers_stat,
            }
            records.append(entity)

    return records


def get_authors_with_stat(q):
    q = add_author_stat_columns(q)
    return execute_with_ministat(q)


def get_topics_with_stat(q):
    q = add_topic_stat_columns(q)
    return execute_with_ministat(q)


def query_follows(author_id: int):
    subquery_shout_author = (
        select(
            [
                ShoutAuthor.author,
                func.count(distinct(ShoutAuthor.shout)).label('shouts_stat'),
            ]
        )
        .group_by(ShoutAuthor.author)
        .where(ShoutAuthor.author == author_id)
        .alias()
    )

    subquery_author_followers = (
        select(
            [
                AuthorFollower.author,
                func.count(distinct(AuthorFollower.author)).label('authors_stat'),
            ]
        )
        .group_by(AuthorFollower.author)
        .where(AuthorFollower.author == author_id)
        .alias()
    )

    subquery_author_followers = (
        select(
            [
                AuthorFollower.follower,
                func.count(distinct(AuthorFollower.follower)).label('followers_stat'),
            ]
        )
        .group_by(AuthorFollower.follower)
        .where(AuthorFollower.follower == author_id)
        .alias()
    )

    subq_shout_author_alias = alias(subquery_shout_author)
    subq_author_followers_alias = alias(
        subquery_author_followers, name='subq_author_followers'
    )
    subq_author_authors_alias = alias(
        subquery_author_followers, name='subq_author_authors'
    )

    authors_query = (
        select(
            [
                Author.id,
                subq_shout_author_alias.c.shouts_stat,
                subq_author_authors_alias.c.authors_stat,
                subq_author_followers_alias.c.followers_stat,
            ]
        )
        .select_from(Author)
        .outerjoin(
            subq_shout_author_alias, Author.id == subq_shout_author_alias.c.author
        )
        .outerjoin(
            subq_author_authors_alias, Author.id == subq_author_followers_alias.c.author
        )
        .outerjoin(
            subq_author_followers_alias,
            Author.id == subq_author_followers_alias.c.follower,
        )
    )

    authors = execute_with_ministat(authors_query)

    subquery_shout_topic = (
        select(
            [
                ShoutTopic.topic,
                func.count(distinct(ShoutTopic.shout)).label('shouts_stat'),
            ]
        )
        .group_by(ShoutTopic.topic)
        .alias()
    )

    subquery_shout_topic_authors = (
        select(
            [
                ShoutTopic.topic,
                func.count(distinct(ShoutTopic.author)).label('authors_stat'),
            ]
        )
        .group_by(ShoutTopic.topic)
        .alias()
    )

    subquery_topic_followers = (
        select(
            [
                TopicFollower.topic,
                func.count(distinct(TopicFollower.follower)).label('followers_stat'),
            ]
        )
        .group_by(TopicFollower.topic_id)
        .alias()
    )

    subq_shout_topic_alias = alias(subquery_shout_topic)
    subq_shout_topic_authors_alias = alias(
        subquery_shout_topic_authors, name='subq_shout_topic_authors'
    )
    subq_topic_followers_alias = alias(
        subquery_topic_followers, name='subq_topic_followers'
    )

    topics_query = (
        select(
            [
                Topic.id,
                subq_shout_topic_alias.columns.shouts_stat,
                subq_shout_topic_authors_alias.columns.authors_stat,
                subq_topic_followers_alias.columns.followers_stat,
            ]
        )
        .select_from(Topic)
        .outerjoin(
            subq_shout_topic_alias, Topic.id == subq_shout_topic_alias.columns.topic_id
        )
        .outerjoin(
            subq_shout_topic_authors_alias,
            Topic.id == subq_shout_topic_authors_alias.columns.topic_id,
        )
        .outerjoin(
            subq_topic_followers_alias,
            Topic.id == subq_topic_followers_alias.columns.topic_id,
        )
    )

    topics = execute_with_ministat(topics_query)

    return {
        'topics': topics,
        'authors': authors,
        'communities': [{'id': 1, 'name': 'Дискурс', 'slug': 'discours'}],
    }
