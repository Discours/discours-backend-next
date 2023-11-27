import time
from enum import Enum as Enumeration

from sqlalchemy import Column, Enum, ForeignKey, Integer, String

from services.db import Base


class ReactionKind(Enumeration):
    # TYPE = <reaction index> # rating diff

    # editor mode
    AGREE = 1  # +1
    DISAGREE = 2  # -1
    ASK = 3  # +0
    PROPOSE = 4  # +0
    PROOF = 5  # +1
    DISPROOF = 6  # -1
    ACCEPT = 7  # +1
    REJECT = 8  # -1

    # public feed
    QUOTE = 9  # +0 bookmark
    COMMENT = 0  # +0
    LIKE = 11  # +1
    DISLIKE = 12  # -1


class Reaction(Base):
    __tablename__ = "reaction"

    body = Column(String, nullable=True, comment="Reaction Body")
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    created_by = Column(ForeignKey("author.id"), nullable=False, index=True)
    updated_at = Column(Integer, nullable=True, comment="Updated at")
    deleted_at = Column(Integer, nullable=True, comment="Deleted at")
    deleted_by = Column(ForeignKey("author.id"), nullable=True, index=True)
    shout = Column(ForeignKey("shout.id"), nullable=False, index=True)
    reply_to = Column(ForeignKey("reaction.id"), nullable=True)
    quote = Column(String, nullable=True, comment="Original quoted text")
    kind = Column(Enum(ReactionKind), nullable=False)

    oid = Column(String)
