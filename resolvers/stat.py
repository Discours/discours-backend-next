from sqlalchemy import func, distinct, select, join, and_, case, true, cast, Integer
from sqlalchemy.orm import aliased

from orm.reaction import Reaction, ReactionKind
from orm.topic import TopicFollower, Topic
from services.db import local_session
from orm.author import AuthorFollower, Author, AuthorRating
from orm.shout import ShoutTopic, ShoutAuthor, Shout


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
    sub_comments = (
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

    q = q.outerjoin(sub_comments, aliased_shout_topic.shout == sub_comments.c.id)
    q = q.add_columns(
        func.coalesce(func.sum(sub_comments.c.comments_count), 0).label('comments_stat')
    )

    q = q.group_by(Topic.id)

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
            Author.id, func.coalesce(func.count(Reaction.id), 0).label('comments_stat')
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
    q = q.add_columns(sub_comments.c.comments_stat)

    # Create a subquery for topics
    sub_topics = (
        select(
            ShoutAuthor.author,
            func.count(distinct(ShoutTopic.topic)).label('topics_stat'),
        )
        .join(Shout, ShoutTopic.shout == Shout.id)
        .join(ShoutAuthor, Shout.id == ShoutAuthor.shout)
        .group_by(ShoutAuthor.author)
        .subquery()
    )

    q = q.outerjoin(sub_topics, Author.id == sub_topics.c.author)
    q = q.add_columns(sub_topics.c.topics_stat)

    q = q.group_by(Author.id, sub_comments.c.comments_stat, sub_topics.c.topics_stat)

    return q


def add_author_ratings(q):
    aliased_author = aliased(Author)
    ratings_subquery = (
        select(
            [
                aliased_author.id.label('author_id'),
                func.count()
                .filter(
                    and_(
                        Reaction.created_by == aliased_author.id,
                        Reaction.kind == ReactionKind.COMMENT.value,
                        Reaction.deleted_at.is_(None),
                    )
                )
                .label('comments_count'),
                func.sum(case((AuthorRating.plus == true(), 1), else_=0)).label(
                    'likes_count'
                ),
                func.sum(case((AuthorRating.plus != true(), 1), else_=0)).label(
                    'dislikes_count'
                ),
                func.sum(
                    case(
                        (
                            and_(
                                Reaction.kind == ReactionKind.LIKE.value,
                                Shout.authors.any(id=aliased_author.id),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label('shouts_likes'),
                func.sum(
                    case(
                        (
                            and_(
                                Reaction.kind == ReactionKind.DISLIKE.value,
                                Shout.authors.any(id=aliased_author.id),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label('shouts_dislikes'),
            ]
        )
        .select_from(aliased_author)
        .join(AuthorRating, cast(AuthorRating.author, Integer) == aliased_author.id)
        .outerjoin(Shout, Shout.authors.any(id=aliased_author.id))
        .filter(Reaction.deleted_at.is_(None))
        .group_by(aliased_author.id)
        .alias('ratings_subquery')
    )

    return q.join(ratings_subquery, Author.id == ratings_subquery.c.author_id)


def get_with_stat(q):
    is_author = f'{q}'.lower().startswith('select author')
    is_topic = f'{q}'.lower().startswith('select topic')
    if is_author:
        q = add_author_stat_columns(q)
        # q = add_author_ratings(q)  # TODO: move rating to cols down there
    elif is_topic:
        q = add_topic_stat_columns(q)
    records = []
    # logger.debug(f'{q}'.replace('\n', ' '))
    with local_session() as session:
        for cols in session.execute(q):
            entity = cols[0]
            entity.stat = {}
            entity.stat['shouts'] = cols[1]
            entity.stat['authors'] = cols[2]
            entity.stat['followers'] = cols[3]
            entity.stat['comments'] = cols[4]
            if is_author:
                entity.stat['topics'] = cols[5]
                # entity.stat['rating'] = cols[5] - cols[6]
                # entity.stat['rating_shouts'] = cols[7] - cols[8]

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
