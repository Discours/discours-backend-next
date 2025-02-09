from services.db import REGISTRY, Base, local_session
from utils.logger import root_logger as logger

from sqlalchemy.types import TypeDecorator
from sqlalchemy.types import String
from sqlalchemy import Column, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import relationship

class ClassType(TypeDecorator):
    impl = String

    @property
    def python_type(self):
        return NotImplemented

    def process_literal_param(self, value, dialect):
        return NotImplemented

    def process_bind_param(self, value, dialect):
        return value.__name__ if isinstance(value, type) else str(value)

    def process_result_value(self, value, dialect):
        class_ = REGISTRY.get(value)
        if class_ is None:
            logger.warn(f"Can't find class <{value}>,find it yourself!", stacklevel=2)
        return class_


class Role(Base):
    __tablename__ = "role"

    name = Column(String, nullable=False, comment="Role Name")
    desc = Column(String, nullable=True, comment="Role Description")
    community = Column(
        ForeignKey("community.id", ondelete="CASCADE"),
        nullable=False,
        comment="Community",
    )
    permissions = relationship(lambda: Permission)

    @staticmethod
    def init_table():
        with local_session() as session:
            r = session.query(Role).filter(Role.name == "author").first()
            if r:
                Role.default_role = r
                return

        r1 = Role.create(
            name="author",
            desc="Role for an author",
            community=1,
        )

        session.add(r1)

        Role.default_role = r1

        r2 = Role.create(
            name="reader",
            desc="Role for a reader",
            community=1,
        )

        session.add(r2)

        r3 = Role.create(
            name="expert",
            desc="Role for an expert",
            community=1,
        )

        session.add(r3)

        r4 = Role.create(
            name="editor",
            desc="Role for an editor",
            community=1,
        )

        session.add(r4)


class Operation(Base):
    __tablename__ = "operation"
    name = Column(String, nullable=False, unique=True, comment="Operation Name")

    @staticmethod
    def init_table():
        with local_session() as session:
            for name in ["create", "update", "delete", "load"]:
                """
                * everyone can:
                    - load shouts
                    - load topics
                    - load reactions
                    - create an account to become a READER
                * readers can:
                    - update and delete their account
                    - load chats
                    - load messages
                    - create reaction of some shout's author allowed kinds
                    - create shout to become an AUTHOR
                * authors can:
                    - update and delete their shout
                    - invite other authors to edit shout and chat
                    - manage allowed reactions for their shout
                * pros can:
                    - create/update/delete their community
                    - create/update/delete topics for their community

                """
                op = session.query(Operation).filter(Operation.name == name).first()
                if not op:
                    op = Operation.create(name=name)
                    session.add(op)
            session.commit()


class Resource(Base):
    __tablename__ = "resource"
    resourceClass = Column(String, nullable=False, unique=True, comment="Resource class")
    name = Column(String, nullable=False, unique=True, comment="Resource name")
    # TODO: community = Column(ForeignKey())

    @staticmethod
    def init_table():
        with local_session() as session:
            for res in [
                "shout",
                "topic",
                "reaction",
                "chat",
                "message",
                "invite",
                "community",
                "user",
            ]:
                r = session.query(Resource).filter(Resource.name == res).first()
                if not r:
                    r = Resource.create(name=res, resourceClass=res)
                    session.add(r)
            session.commit()


class Permission(Base):
    __tablename__ = "permission"
    __table_args__ = (
        UniqueConstraint("role", "operation", "resource"),
        {"extend_existing": True},
    )

    role: Column = Column(ForeignKey("role.id", ondelete="CASCADE"), nullable=False, comment="Role")
    operation: Column = Column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        nullable=False,
        comment="Operation",
    )
    resource: Column = Column(
        ForeignKey("resource.id", ondelete="CASCADE"),
        nullable=False,
        comment="Resource",
    )


# if __name__ == "__main__":
#     Base.metadata.create_all(engine)
#     ops = [
#         Permission(role=1, operation=1, resource=1),
#         Permission(role=1, operation=2, resource=1),
#         Permission(role=1, operation=3, resource=1),
#         Permission(role=1, operation=4, resource=1),
#         Permission(role=2, operation=4, resource=1),
#     ]
#     global_session.add_all(ops)
#     global_session.commit()