from dataclasses import dataclass
from typing import Any, Dict, List, Optional

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

    @classmethod
    def ok(cls, data: Dict[str, Any]) -> "CommonResult":
        """
        Создает успешный результат.

        Args:
            data: Словарь с данными для включения в результат.

        Returns:
            CommonResult: Экземпляр с предоставленными данными.
        """
        result = cls()
        for key, value in data.items():
            if hasattr(result, key):
                setattr(result, key, value)
        return result

    @classmethod
    def error(cls, message: str):
        """
        Create an error result.

        Args:
            message: The error message.

        Returns:
            CommonResult: An instance with the error message.
        """
        return cls(error=message)
