from sqlalchemy import event
from orm.author import Author, AuthorFollower
from orm.reaction import Reaction
from orm.shout import Shout, ShoutAuthor
from orm.topic import Topic, TopicFollower
from cache.revalidator import revalidation_manager
from utils.logger import root_logger as logger


def after_update_handler(mapper, connection, target):
    """Обработчик обновления сущности."""
    entity_type = "authors" if isinstance(target, Author) else "topics" if isinstance(target, Topic) else "shouts"
    revalidation_manager.mark_for_revalidation(target.id, entity_type)


def after_follower_insert_update_handler(mapper, connection, target):
    """Обработчик добавления или обновления подписки."""
    if isinstance(target, AuthorFollower):
        # Пометить автора и подписчика для ревалидации
        revalidation_manager.mark_for_revalidation(target.author_id, "authors")
        revalidation_manager.mark_for_revalidation(target.follower_id, "authors")
    elif isinstance(target, TopicFollower):
        # Пометить тему и подписчика для ревалидации
        revalidation_manager.mark_for_revalidation(target.topic_id, "topics")
        revalidation_manager.mark_for_revalidation(target.follower_id, "authors")


def after_follower_delete_handler(mapper, connection, target):
    """Обработчик удаления подписки."""
    if isinstance(target, AuthorFollower):
        # Пометить автора и подписчика для ревалидации
        revalidation_manager.mark_for_revalidation(target.author_id, "authors")
        revalidation_manager.mark_for_revalidation(target.follower_id, "authors")
    elif isinstance(target, TopicFollower):
        # Пометить тему и подписчика для ревалидации
        revalidation_manager.mark_for_revalidation(target.topic_id, "topics")
        revalidation_manager.mark_for_revalidation(target.follower_id, "authors")


def after_reaction_update_handler(mapper, connection, reaction):
    """Обработчик изменений реакций."""
    # Пометить shout для ревалидации
    revalidation_manager.mark_for_revalidation(reaction.shout_id, "shouts")
    # Пометить автора реакции для ревалидации
    revalidation_manager.mark_for_revalidation(reaction.created_by, "authors")


def after_shout_author_insert_update_handler(mapper, connection, target):
    """Обработчик добавления или обновления авторства публикации."""
    # Пометить shout и автора для ревалидации
    revalidation_manager.mark_for_revalidation(target.shout_id, "shouts")
    revalidation_manager.mark_for_revalidation(target.author_id, "authors")


def after_shout_author_delete_handler(mapper, connection, target):
    """Обработчик удаления авторства публикации."""
    # Пометить shout и автора для ревалидации
    revalidation_manager.mark_for_revalidation(target.shout_id, "shouts")
    revalidation_manager.mark_for_revalidation(target.author_id, "authors")


def events_register():
    """Регистрация обработчиков событий для всех сущностей."""
    event.listen(ShoutAuthor, "after_insert", after_shout_author_insert_update_handler)
    event.listen(ShoutAuthor, "after_update", after_shout_author_insert_update_handler)
    event.listen(ShoutAuthor, "after_delete", after_shout_author_delete_handler)

    event.listen(AuthorFollower, "after_insert", after_follower_insert_update_handler)
    event.listen(AuthorFollower, "after_update", after_follower_insert_update_handler)
    event.listen(AuthorFollower, "after_delete", after_follower_delete_handler)
    event.listen(TopicFollower, "after_insert", after_follower_insert_update_handler)
    event.listen(TopicFollower, "after_update", after_follower_insert_update_handler)
    event.listen(TopicFollower, "after_delete", after_follower_delete_handler)
    event.listen(Reaction, "after_update", after_reaction_update_handler)

    event.listen(Author, "after_update", after_update_handler)
    event.listen(Topic, "after_update", after_update_handler)
    event.listen(Shout, "after_update", after_update_handler)
    event.listen(
        Reaction,
        "after_update",
        lambda mapper, connection, target: revalidation_manager.mark_for_revalidation(target.shout, "shouts"),
    )

    logger.info("Event handlers registered successfully.")
