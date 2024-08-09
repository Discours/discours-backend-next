from sqlalchemy import event

from cache.revalidator import revalidation_manager
from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from utils.logger import root_logger as logger


def mark_for_revalidation(entity, *args):
    """Отметка сущности для ревалидации."""
    entity_type = (
        "authors"
        if isinstance(entity, Author)
        else "topics"
        if isinstance(entity, Topic)
        else "reactions"
        if isinstance(entity, Reaction)
        else "shouts"
        if isinstance(entity, Shout)
        else None
    )
    if entity_type:
        revalidation_manager.mark_for_revalidation(entity.id, entity_type)


def after_follower_handler(mapper, connection, target, is_delete=False):
    """Обработчик добавления, обновления или удаления подписки."""
    entity_type = None
    if isinstance(target, AuthorFollower):
        entity_type = "authors"
    elif isinstance(target, TopicFollower):
        entity_type = "topics"
    elif isinstance(target, ShoutReactionsFollower):
        entity_type = "shouts"

    if entity_type:
        revalidation_manager.mark_for_revalidation(
            target.author_id if entity_type == "authors" else target.topic_id, entity_type
        )
        if not is_delete:
            revalidation_manager.mark_for_revalidation(target.follower_id, "authors")


def events_register():
    """Регистрация обработчиков событий для всех сущностей."""
    event.listen(ShoutAuthor, "after_insert", mark_for_revalidation)
    event.listen(ShoutAuthor, "after_update", mark_for_revalidation)
    event.listen(ShoutAuthor, "after_delete", mark_for_revalidation)

    event.listen(AuthorFollower, "after_insert", after_follower_handler)
    event.listen(AuthorFollower, "after_update", after_follower_handler)
    event.listen(AuthorFollower, "after_delete", lambda *args: after_follower_handler(*args, is_delete=True))

    event.listen(TopicFollower, "after_insert", after_follower_handler)
    event.listen(TopicFollower, "after_update", after_follower_handler)
    event.listen(TopicFollower, "after_delete", lambda *args: after_follower_handler(*args, is_delete=True))

    event.listen(ShoutReactionsFollower, "after_insert", after_follower_handler)
    event.listen(ShoutReactionsFollower, "after_update", after_follower_handler)
    event.listen(ShoutReactionsFollower, "after_delete", lambda *args: after_follower_handler(*args, is_delete=True))

    event.listen(Reaction, "after_update", mark_for_revalidation)
    event.listen(Author, "after_update", mark_for_revalidation)
    event.listen(Topic, "after_update", mark_for_revalidation)
    event.listen(Shout, "after_update", mark_for_revalidation)

    logger.info("Event handlers registered successfully.")
