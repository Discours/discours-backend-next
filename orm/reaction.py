from datetime import datetime
from enum import Enum as Enumeration
from sqlalchemy import Column, DateTime, Enum, ForeignKey, String
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
    createdAt = Column(DateTime, nullable=False, default=datetime.now)
    createdBy = Column(ForeignKey("author.id"), nullable=False, index=True)
    updatedAt = Column(DateTime, nullable=True, comment="Updated at")
    updatedBy = Column(ForeignKey("author.id"), nullable=True, index=True)
    deletedAt = Column(DateTime, nullable=True, comment="Deleted at")
    deletedBy = Column(ForeignKey("author.id"), nullable=True, index=True)
    shout = Column(ForeignKey("shout.id"), nullable=False, index=True)
    replyTo = Column(ForeignKey("reaction.id"), nullable=True)
    range = Column(String, nullable=True, comment="<start index>:<end>")
    kind = Column(Enum(ReactionKind), nullable=False)
