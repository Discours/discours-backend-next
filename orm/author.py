import time

from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy import event

from services.db import Base


class AuthorRating(Base):
    __tablename__ = 'author_rating'

    id = None  # type: ignore
    rater = Column(ForeignKey('author.id'), primary_key=True)
    author = Column(ForeignKey('author.id'), primary_key=True)
    plus = Column(Boolean)


class AuthorFollower(Base):
    __tablename__ = 'author_follower'

    id = None  # type: ignore
    follower = Column(ForeignKey('author.id'), primary_key=True)
    author = Column(ForeignKey('author.id'), primary_key=True)
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    auto = Column(Boolean, nullable=False, default=False)


class Author(Base):
    __tablename__ = 'author'

    user = Column(String,
    )  # unbounded link with authorizer's User type

    name = Column(String, nullable=True, comment='Display name')
    slug = Column(String, unique=True, comment="Author's slug")
    bio = Column(String, nullable=True, comment='Bio')  # status description
    about = Column(String, nullable=True, comment='About')  # long and formatted
    pic = Column(String, nullable=True, comment='Picture')
    links = Column(JSON, nullable=True, comment='Links')
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    last_seen = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    deleted_at = Column(Integer, nullable=True, comment='Deleted at')

def get_object(connection, table_name, object_id):
    return connection.execute(f"SELECT * FROM {table_name} WHERE id = :object_id", {"object_id": object_id}).fetchone()

def update_app_data(connection, user_id, app_data):
    connection.execute("UPDATE authorizer_users SET app_data = :app_data WHERE id = :user_id", {"app_data": app_data, "user_id": user_id})

def update_follows(user, entity_type, entity):
    app_data = user.app_data or {}
    app_data['follows'] = user.app_data or {"topics": [], "authors": [], "shouts": [], "communities": []}
    app_data['follows'][f'{entity_type}s'].append(vars(entity))
    return app_data

@event.listens_for(Author, 'after_insert')
@event.listens_for(Author, 'after_update')
def after_author_update(mapper, connection, target):
    user_id = target.user
    user = get_object(connection, 'authorizer_users', user_id)
    if user:
        app_data = update_follows(user, 'author', target)
        update_app_data(connection, user_id, app_data)


@event.listens_for(AuthorFollower, 'after_insert')
@event.listens_for(AuthorFollower, 'after_delete')
def after_author_follower_change(mapper, connection, target):
    author_id = target.author
    follower_id = target.follower
    user = get_object(connection, 'authorizer_users', follower_id)
    if user:
        app_data = update_follows(user, 'author', get_object(connection, 'author', author_id))
        update_app_data(connection, follower_id, app_data)
