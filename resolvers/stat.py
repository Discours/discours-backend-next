from sqlalchemy import func, distinct

from services.db import local_session
from orm.author import AuthorFollower
from orm.shout import ShoutTopic, ShoutAuthor


def add_stat_columns(q, author_alias, follower_model_alias):
    shouts_stat_model = ShoutAuthor if isinstance(follower_model_alias, AuthorFollower) else ShoutTopic
    q = q.outerjoin(shouts_stat_model).add_columns(func.count(distinct(shouts_stat_model.shout)).label('shouts_stat'))
    q = q.outerjoin(
        follower_model_alias, follower_model_alias.follower == author_alias.id
    ).add_columns(func.count(distinct(follower_model_alias.author)).label('authors_stat'))
    q = q.outerjoin(follower_model_alias, follower_model_alias.author == author_alias.id).add_columns(
        func.count(distinct(follower_model_alias.follower)).label('followers_stat')
    )
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


def get_with_stat(q, author_alias, follower_model_alias):
    q = add_stat_columns(q, author_alias, follower_model_alias)
    return unpack_stat(q)
