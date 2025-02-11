import time

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import relationship

from services.db import Base


class Permission(Base):
    __tablename__ = "permission"

    id = Column(String, primary_key=True, unique=True, nullable=False, default=None)
    resource = Column(String, nullable=False)
    operation = Column(String, nullable=False)


class Role(Base):
    __tablename__ = "role"

    id = Column(String, primary_key=True, unique=True, nullable=False, default=None)
    name = Column(String, nullable=False)
    permissions = relationship(Permission)


class AuthorizerUser(Base):
    __tablename__ = "authorizer_users"

    id = Column(String, primary_key=True, unique=True, nullable=False, default=None)
    key = Column(String)
    email = Column(String, unique=True)
    email_verified_at = Column(Integer)
    family_name = Column(String)
    gender = Column(String)
    given_name = Column(String)
    is_multi_factor_auth_enabled = Column(Boolean)
    middle_name = Column(String)
    nickname = Column(String)
    password = Column(String)
    phone_number = Column(String, unique=True)
    phone_number_verified_at = Column(Integer)
    # preferred_username = Column(String, nullable=False)
    picture = Column(String)
    revoked_timestamp = Column(Integer)
    roles = Column(String, default="author,reader")
    signup_methods = Column(String, default="magic_link_login")
    created_at = Column(Integer, default=lambda: int(time.time()))
    updated_at = Column(Integer, default=lambda: int(time.time()))


class UserRating(Base):
    __tablename__ = "user_rating"

    id = None
    rater: Column = Column(ForeignKey("user.id"), primary_key=True, index=True)
    user: Column = Column(ForeignKey("user.id"), primary_key=True, index=True)
    value: Column = Column(Integer)

    @staticmethod
    def init_table():
        pass


class UserRole(Base):
    __tablename__ = "user_role"

    id = None
    user = Column(ForeignKey("user.id"), primary_key=True, index=True)
    role = Column(ForeignKey("role.id"), primary_key=True, index=True)


class User(Base):
    __tablename__ = "user"
    default_user = None

    email = Column(String, unique=True, nullable=False, comment="Email")
    username = Column(String, nullable=False, comment="Login")
    password = Column(String, nullable=True, comment="Password")
    bio = Column(String, nullable=True, comment="Bio")  # status description
    about = Column(String, nullable=True, comment="About")  # long and formatted
    userpic = Column(String, nullable=True, comment="Userpic")
    name = Column(String, nullable=True, comment="Display name")
    slug = Column(String, unique=True, comment="User's slug")
    links = Column(JSON, nullable=True, comment="Links")
    oauth = Column(String, nullable=True)
    oid = Column(String, nullable=True)

    muted = Column(Boolean, default=False)
    confirmed = Column(Boolean, default=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), comment="Created at")
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), comment="Updated at")
    last_seen = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), comment="Was online at")
    deleted_at = Column(DateTime(timezone=True), nullable=True, comment="Deleted at")

    ratings = relationship(UserRating, foreign_keys=UserRating.user)
    roles = relationship(lambda: Role, secondary=UserRole.__tablename__)

    def get_permission(self):
        scope = {}
        for role in self.roles:
            for p in role.permissions:
                if p.resource not in scope:
                    scope[p.resource] = set()
                scope[p.resource].add(p.operation)
        print(scope)
        return scope


# if __name__ == "__main__":
#   print(User.get_permission(user_id=1))
