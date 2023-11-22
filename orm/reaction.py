import time
from enum import Enum as Enumeration

from sqlalchemy import Column, Enum, ForeignKey, Integer, String

from services.db import Base


class ReactionKind(Enumeration):
    AGREE = 1  # +1
    DISAGREE = 2  # -1
    PROOF = 3  # +1
    DISPROOF = 4  # -1
    ASK = 5  # +0
    PROPOSE = 6  # +0
    QUOTE = 7  # +0 bookmark
    COMMENT = 8  # +0
    ACCEPT = 9  # +1
    REJECT = 0  # -1
    LIKE = 11  # +1
    DISLIKE = 12  # -1
    REMARK = 13  # 0
    FOOTNOTE = 14  # 0
    # TYPE = <reaction index> # rating diff


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
    range = Column(String, nullable=True, comment="<start index>:<end>")
    kind = Column(Enum(ReactionKind), nullable=False)

    oid = Column(String)
