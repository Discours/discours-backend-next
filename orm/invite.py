import enum

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship

from services.db import Base


class InviteStatus(enum.Enum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"

    @classmethod
    def from_string(cls, value):
        return cls(value)


class Invite(Base):
    __tablename__ = "invite"

    inviter_id = Column(ForeignKey("author.id"), primary_key=True)
    author_id = Column(ForeignKey("author.id"), primary_key=True)
    shout_id = Column(ForeignKey("shout.id"), primary_key=True)
    status = Column(String, default=InviteStatus.PENDING.value)

    inviter = relationship("Author", foreign_keys=[inviter_id])
    author = relationship("Author", foreign_keys=[author_id])
    shout = relationship("Shout")

    def set_status(self, status: InviteStatus):
        self.status = status.value

    def get_status(self) -> InviteStatus:
        return InviteStatus.from_string(self.status)
