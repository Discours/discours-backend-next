from dataclasses import dataclass
from typing import List, Optional

from orm.author import Author
from orm.community import Community
from orm.reaction import Reaction
from orm.shout import Shout
from orm.topic import Topic


@dataclass
class CommonResult:
    error: Optional[str] = None
    slugs: Optional[List[str]] = None
    shout: Optional[Shout] = None
    shouts: Optional[List[Shout]] = None
    author: Optional[Author] = None
    authors: Optional[List[Author]] = None
    reaction: Optional[Reaction] = None
    reactions: Optional[List[Reaction]] = None
    topic: Optional[Topic] = None
    topics: Optional[List[Topic]] = None
    community: Optional[Community] = None
    communities: Optional[List[Community]] = None
