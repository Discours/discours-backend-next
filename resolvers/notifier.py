import json
import time
from typing import List, Tuple

from sqlalchemy import and_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import aliased
from sqlalchemy.sql import not_

from orm.author import Author
from orm.notification import (
    Notification,
    NotificationAction,
    NotificationEntity,
    NotificationSeen,
)
from orm.shout import Shout
from services.auth import login_required
from services.db import local_session
from services.logger import root_logger as logger
from services.schema import mutation, query


def query_notifications(
    author_id: int, after: int = 0
) -> Tuple[int, int, List[Tuple[Notification, bool]]]:
    notification_seen_alias = aliased(NotificationSeen)
    q = select(Notification, notification_seen_alias.viewer.label("seen")).outerjoin(
        NotificationSeen,
        and_(
            NotificationSeen.viewer == author_id,
            NotificationSeen.notification == Notification.id,
        ),
    )
    if after:
        q = q.filter(Notification.created_at > after)
    q = q.group_by(NotificationSeen.notification, Notification.created_at)

    with local_session() as session:
        total = (
            session.query(Notification)
            .filter(
                and_(
                    Notification.action == NotificationAction.CREATE.value,
                    Notification.created_at > after,
                )
            )
            .count()
        )

        unread = (
            session.query(Notification)
            .filter(
                and_(
                    Notification.action == NotificationAction.CREATE.value,
                    Notification.created_at > after,
                    not_(Notification.seen),
                )
            )
            .count()
        )

        notifications_result = session.execute(q)
        notifications = []
        for n, seen in notifications_result:
            notifications.append((n, seen))

    return total, unread, notifications


def group_notification(
    thread, authors=None, shout=None, reactions=None, entity="follower", action="follow"
):
    reactions = reactions or []
    authors = authors or []
    return {
        "thread": thread,
        "authors": authors,
        "updated_at": int(time.time()),
        "shout": shout,
        "reactions": reactions,
        "entity": entity,
        "action": action,
    }


def get_notifications_grouped(
    author_id: int, after: int = 0, limit: int = 10, offset: int = 0
):
    """
    Retrieves notifications for a given author.

    Args:
        author_id (int): The ID of the author for whom notifications are retrieved.
        after (int, optional): If provided, selects only notifications created after this timestamp will be considered.
        limit (int, optional): The maximum number of groupa to retrieve.
        offset (int, optional): offset

    Returns:
        Dict[str, NotificationGroup], int, int: A dictionary where keys are thread IDs
        and values are NotificationGroup objects, unread and total amounts.

    This function queries the database to retrieve notifications for the specified author, considering optional filters.
    The result is a dictionary where each key is a thread ID, and the corresponding value is a NotificationGroup
    containing information about the notifications within that thread.

    NotificationGroup structure:
    {
        entity: str,        # Type of entity (e.g., 'reaction', 'shout', 'follower').
        updated_at: int,    # Timestamp of the latest update in the thread.
        shout: Optional[NotificationShout]
        reactions: List[int],  # List of reaction ids within the thread.
        authors: List[NotificationAuthor],  # List of authors involved in the thread.
    }
    """
    total, unread, notifications = query_notifications(author_id, after)
    groups_by_thread = {}
    groups_amount = 0

    for notification, seen in notifications:
        if (groups_amount + offset) >= limit:
            break

        payload = json.loads(str(notification.payload))

        if str(notification.entity) == NotificationEntity.SHOUT.value:
            shout = payload
            shout_id = shout.get("id")
            author_id = shout.get("created_by")
            thread_id = f"shout-{shout_id}"
            with local_session() as session:
                author = session.query(Author).filter(Author.id == author_id).first()
                shout = session.query(Shout).filter(Shout.id == shout_id).first()
                if author and shout:
                    author = author.dict()
                    shout = shout.dict()
                    group = group_notification(
                        thread_id,
                        shout=shout,
                        authors=[author],
                        action=str(notification.action),
                        entity=str(notification.entity),
                    )
                    groups_by_thread[thread_id] = group
                    groups_amount += 1

        elif str(notification.entity) == NotificationEntity.REACTION.value:
            reaction = payload
            if not isinstance(shout, dict):
                raise ValueError("reaction data is not consistent")
            shout_id = shout.get("shout")
            author_id = shout.get("created_by", 0)
            if shout_id and author_id:
                with local_session() as session:
                    author = (
                        session.query(Author).filter(Author.id == author_id).first()
                    )
                    shout = session.query(Shout).filter(Shout.id == shout_id).first()
                    if shout and author:
                        author = author.dict()
                        shout = shout.dict()
                        reply_id = reaction.get("reply_to")
                        thread_id = f"shout-{shout_id}"
                        if reply_id and reaction.get("kind", "").lower() == "comment":
                            thread_id += f"{reply_id}"
                        existing_group = groups_by_thread.get(thread_id)
                        if existing_group:
                            existing_group["seen"] = False
                            existing_group["authors"].append(author_id)
                            existing_group["reactions"] = (
                                existing_group["reactions"] or []
                            )
                            existing_group["reactions"].append(reaction)
                            groups_by_thread[thread_id] = existing_group
                        else:
                            group = group_notification(
                                thread_id,
                                authors=[author],
                                shout=shout,
                                reactions=[reaction],
                                entity=str(notification.entity),
                                action=str(notification.action),
                            )
                            if group:
                                groups_by_thread[thread_id] = group
                                groups_amount += 1

            elif str(notification.entity) == "follower":
                thread_id = "followers"
                follower = json.loads(payload)
                group = groups_by_thread.get(thread_id)
                if group:
                    if str(notification.action) == "follow":
                        group["authors"].append(follower)
                    elif str(notification.action) == "unfollow":
                        follower_id = follower.get("id")
                        for author in group["authors"]:
                            if author.get("id") == follower_id:
                                group["authors"].remove(author)
                                break
                else:
                    group = group_notification(
                        thread_id,
                        authors=[follower],
                        entity=str(notification.entity),
                        action=str(notification.action),
                    )
                    groups_amount += 1
                groups_by_thread[thread_id] = group
    return groups_by_thread, unread, total


@query.field("load_notifications")
@login_required
async def load_notifications(_, info, after: int, limit: int = 50, offset=0):
    author_id = info.context.get("author_id")
    error = None
    total = 0
    unread = 0
    notifications = []
    try:
        if author_id:
            groups, unread, total = get_notifications_grouped(author_id, after, limit)
            notifications = sorted(
                groups.values(), key=lambda group: group.updated_at, reverse=True
            )
    except Exception as e:
        error = e
        logger.error(e)
    return {
        "notifications": notifications,
        "total": total,
        "unread": unread,
        "error": error,
    }


@mutation.field("notification_mark_seen")
@login_required
async def notification_mark_seen(_, info, notification_id: int):
    author_id = info.context.get("author_id")
    if author_id:
        with local_session() as session:
            try:
                ns = NotificationSeen(notification=notification_id, viewer=author_id)
                session.add(ns)
                session.commit()
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"seen mutation failed: {e}")
                return {"error": "cant mark as read"}
    return {"error": None}


@mutation.field("notifications_seen_after")
@login_required
async def notifications_seen_after(_, info, after: int):
    # TODO: use latest loaded notification_id as input offset parameter
    error = None
    try:
        author_id = info.context.get("author_id")
        if author_id:
            with local_session() as session:
                nnn = (
                    session.query(Notification)
                    .filter(and_(Notification.created_at > after))
                    .all()
                )
                for n in nnn:
                    try:
                        ns = NotificationSeen(notification=n.id, viewer=author_id)
                        session.add(ns)
                        session.commit()
                    except SQLAlchemyError:
                        session.rollback()
    except Exception as e:
        print(e)
        error = "cant mark as read"
    return {"error": error}


@mutation.field("notifications_seen_thread")
@login_required
async def notifications_seen_thread(_, info, thread: str, after: int):
    error = None
    author_id = info.context.get("author_id")
    if author_id:
        [shout_id, reply_to_id] = thread.split(":")
        with local_session() as session:
            # TODO: handle new follower and new shout notifications
            new_reaction_notifications = (
                session.query(Notification)
                .filter(
                    Notification.action == "create",
                    Notification.entity == "reaction",
                    Notification.created_at > after,
                )
                .all()
            )
            removed_reaction_notifications = (
                session.query(Notification)
                .filter(
                    Notification.action == "delete",
                    Notification.entity == "reaction",
                    Notification.created_at > after,
                )
                .all()
            )
            exclude = set()
            for nr in removed_reaction_notifications:
                reaction = json.loads(str(nr.payload))
                reaction_id = reaction.get("id")
                exclude.add(reaction_id)
            for n in new_reaction_notifications:
                reaction = json.loads(str(n.payload))
                reaction_id = reaction.get("id")
                if (
                    reaction_id not in exclude
                    and reaction.get("shout") == shout_id
                    and reaction.get("reply_to") == reply_to_id
                ):
                    try:
                        ns = NotificationSeen(notification=n.id, viewer=author_id)
                        session.add(ns)
                        session.commit()
                    except Exception as e:
                        logger.warn(e)
                        session.rollback()
    else:
        error = "You are not logged in"
    return {"error": error}
