from sqlalchemy import func, distinct, select, join, and_
from sqlalchemy.orm import aliased

from orm.reaction import Reaction, ReactionKind
from orm.topic import TopicFollower, Topic
from services.db import local_session
from orm.author import AuthorFollower, Author, AuthorRating
from orm.shout import ShoutTopic, ShoutAuthor, Shout
from services.logger import root_logger as logger


def add_topic_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_topic_follower = aliased(TopicFollower)
    aliased_shout_topic = aliased(ShoutTopic)

    q = (
        q.outerjoin(aliased_shout_topic, aliased_shout_topic.topic == Topic.id)
        .add_columns(func.count(distinct(aliased_shout_topic.shout)).label("shouts_stat"))
        .outerjoin(aliased_shout_author, aliased_shout_topic.shout == aliased_shout_author.shout)
        .add_columns(func.count(distinct(aliased_shout_author.author)).label("authors_stat"))
        .outerjoin(aliased_topic_follower)
        .add_columns(func.count(distinct(aliased_topic_follower.follower)).label("followers_stat"))
    )

    q = q.group_by(Topic.id)

    return q


def add_author_stat_columns(q):
    aliased_shout_author = aliased(ShoutAuthor)
    aliased_author_authors = aliased(AuthorFollower)
    aliased_author_followers = aliased(AuthorFollower)

    q = (
        q.outerjoin(aliased_shout_author, aliased_shout_author.author == Author.id)
        .add_columns(func.count(distinct(aliased_shout_author.shout)).label("shouts_stat"))
        .outerjoin(aliased_author_authors, aliased_author_authors.follower == Author.id)
        .add_columns(func.count(distinct(aliased_shout_author.author)).label("authors_stat"))
        .outerjoin(aliased_author_followers, aliased_author_followers.author == Author.id)
        .add_columns(func.count(distinct(aliased_author_followers.follower)).label("followers_stat"))
    )

    q = q.group_by(Author.id)

    return q


def count_author_comments_rating(session, author_id) -> int:
    replied_alias = aliased(Reaction)
    replies_likes = (
        session.query(replied_alias)
        .join(Reaction, replied_alias.id == Reaction.reply_to)
        .where(
            and_(
                replied_alias.created_by == author_id,
                replied_alias.kind == ReactionKind.COMMENT.value,
            )
        )
        .filter(replied_alias.kind == ReactionKind.LIKE.value)
        .count()
    ) or 0
    replies_dislikes = (
        session.query(replied_alias)
        .join(Reaction, replied_alias.id == Reaction.reply_to)
        .where(
            and_(
                replied_alias.created_by == author_id,
                replied_alias.kind == ReactionKind.COMMENT.value,
            )
        )
        .filter(replied_alias.kind == ReactionKind.DISLIKE.value)
        .count()
    ) or 0

    return replies_likes - replies_dislikes


def count_author_shouts_rating(session, author_id) -> int:
    shouts_likes = (
        session.query(Reaction, Shout)
        .join(Shout, Shout.id == Reaction.shout)
        .filter(
            and_(
                Shout.authors.any(id=author_id),
                Reaction.kind == ReactionKind.LIKE.value,
            )
        )
        .count()
        or 0
    )
    shouts_dislikes = (
        session.query(Reaction, Shout)
        .join(Shout, Shout.id == Reaction.shout)
        .filter(
            and_(
                Shout.authors.any(id=author_id),
                Reaction.kind == ReactionKind.DISLIKE.value,
            )
        )
        .count()
        or 0
    )
    return shouts_likes - shouts_dislikes


def load_author_ratings(author: Author):
    with local_session() as session:
        comments_count = (
            session.query(Reaction)
            .filter(
                and_(
                    Reaction.created_by == author.id,
                    Reaction.kind == ReactionKind.COMMENT.value,
                    Reaction.deleted_at.is_(None),
                    )
            )
            .count()
        )
        likes_count = (
            session.query(AuthorRating)
            .filter(
                and_(AuthorRating.author == author.id, AuthorRating.plus.is_(True))
            )
            .count()
        )
        dislikes_count = (
            session.query(AuthorRating)
            .filter(
                and_(
                    AuthorRating.author == author.id, AuthorRating.plus.is_not(True)
                )
            )
            .count()
        )
        author.stat['rating'] = likes_count - dislikes_count
        author.stat['rating_shouts'] = count_author_shouts_rating(
            session, author.id
        )
        author.stat['rating_comments'] = count_author_comments_rating(
            session, author.id
        )
        author.stat['commented'] = comments_count
        return author


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


def get_authors_with_stat(q, ratings=False):
    q = add_author_stat_columns(q)
    authors = execute_with_ministat(q)
    if ratings:
        authors_with_ratings = []
        for author in authors:
            authors_with_ratings.append(load_author_ratings(author))
            return authors_with_ratings
    return authors


def get_topics_with_stat(q):
    q = add_topic_stat_columns(q)
    return execute_with_ministat(q)


def author_follows_authors(author_id: int):
    af = aliased(AuthorFollower, name="af")
    q = (
        select(Author).select_from(
            join(Author, af, Author.id == int(af.author))
        ).where(af.follower == author_id)
    )
    q = add_author_stat_columns(q)
    return execute_with_ministat(q)


def author_follows_topics(author_id: int):
    q = (
        select(Topic).select_from(
            join(Topic, TopicFollower, Topic.id == int(TopicFollower.topic))
        ).where(TopicFollower.follower == author_id)
    )

    q = add_topic_stat_columns(q)
    return execute_with_ministat(q)


def query_follows(author_id: int):
    try:
        topics = author_follows_topics(author_id)
        authors = author_follows_authors(author_id)
        return {
            'topics': topics,
            'authors': authors,
            'communities': [{'id': 1, 'name': 'Дискурс', 'slug': 'discours'}],
        }
    except Exception as e:
        logger.exception(f"An error occurred while executing query_follows: {e}")
        raise Exception("An error occurred while executing query_follows") from e
