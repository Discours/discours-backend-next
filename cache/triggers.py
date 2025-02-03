from sqlalchemy import event

from cache.revalidator import revalidation_manager
from orm.author import Author, AuthorFollower
from orm.reaction import Reaction, ReactionKind
from orm.shout import Shout, ShoutAuthor, ShoutReactionsFollower
from orm.topic import Topic, TopicFollower
from utils.logger import root_logger as logger
from services.db import local_session


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
            target.author if entity_type == "authors" else target.topic, entity_type
        )
        if not is_delete:
            revalidation_manager.mark_for_revalidation(target.follower, "authors")


def after_shout_handler(mapper, connection, target):
    """Обработчик изменения статуса публикации"""
    if not isinstance(target, Shout):
        return
        
    # Проверяем изменение статуса публикации
    was_published = target.published_at is not None and target.deleted_at is None
    
    # Всегда обновляем счетчики для авторов и тем при любом изменении поста
    for author in target.authors:
        revalidation_manager.mark_for_revalidation(author.id, "authors")
        
    for topic in target.topics:
        revalidation_manager.mark_for_revalidation(topic.id, "topics")
        
    # Обновляем сам пост
    revalidation_manager.mark_for_revalidation(target.id, "shouts")


def after_reaction_handler(mapper, connection, target):
    """Обработчик для комментариев"""
    if not isinstance(target, Reaction):
        return
        
    # Проверяем что это комментарий
    is_comment = target.kind == ReactionKind.COMMENT.value
    
    # Получаем связанный пост
    shout_id = target.shout if isinstance(target.shout, int) else target.shout.id
    if not shout_id:
        return
        
    # Обновляем счетчики для автора комментария
    if target.created_by:
        revalidation_manager.mark_for_revalidation(target.created_by, "authors")
    
    # Обновляем счетчики для поста
    revalidation_manager.mark_for_revalidation(shout_id, "shouts")
    
    if is_comment:
        # Для комментариев обновляем также авторов и темы
        with local_session() as session:
            shout = session.query(Shout).filter(
                Shout.id == shout_id,
                Shout.published_at.is_not(None),
                Shout.deleted_at.is_(None)
            ).first()
            
            if shout:
                for author in shout.authors:
                    revalidation_manager.mark_for_revalidation(author.id, "authors")
                    
                for topic in shout.topics:
                    revalidation_manager.mark_for_revalidation(topic.id, "topics")


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
    event.listen(Shout, "after_update", after_shout_handler)
    event.listen(Shout, "after_delete", after_shout_handler)

    event.listen(Reaction, "after_insert", after_reaction_handler)
    event.listen(Reaction, "after_update", after_reaction_handler)
    event.listen(Reaction, "after_delete", after_reaction_handler)

    logger.info("Event handlers registered successfully.")
