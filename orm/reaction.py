import time
from enum import Enum as Enumeration

from sqlalchemy import Column, ForeignKey, Integer, String

from services.db import Base


class ReactionKind(Enumeration):
    # TYPE = <reaction index> # rating diff

    # editor mode
    AGREE = "AGREE"  # +1
    DISAGREE = "DISAGREE"  # -1
    ASK = "ASK"  # +0
    PROPOSE = "PROPOSE"  # +0
    ACCEPT = "ACCEPT"  # +1
    REJECT = "REJECT"  # -1

    # expert mode
    PROOF = "PROOF"  # +1
    DISPROOF = "DISPROOF"  # -1

    # public feed
    QUOTE = "QUOTE"  # +0 TODO: use to bookmark in collection
    COMMENT = "COMMENT"  # +0
    LIKE = "LIKE"  # +1
    DISLIKE = "DISLIKE"  # -1


class Reaction(Base):
    __tablename__ = "reaction"

    body = Column(String, default="", comment="Reaction Body")
    created_at = Column(Integer, nullable=False, default=lambda: int(time.time()))
    updated_at = Column(Integer, nullable=True, comment="Updated at")
    deleted_at = Column(Integer, nullable=True, comment="Deleted at")
    deleted_by = Column(ForeignKey("author.id"), nullable=True)
    reply_to = Column(ForeignKey("reaction.id"), nullable=True)
    quote = Column(String, nullable=True, comment="Original quoted text")
    shout = Column(ForeignKey("shout.id"), nullable=False)
    created_by = Column(ForeignKey("author.id"), nullable=False)
    kind = Column(String, nullable=False)

    oid = Column(String)
