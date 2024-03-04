import json
import time
from typing import List, Tuple

from sqlalchemy.exc import SQLAlchemyError

from services.auth import login_required
from services.schema import mutation, query
from sqlalchemy import and_, select
from sqlalchemy.orm import aliased
from sqlalchemy.sql import not_

from orm.notification import (
    Notification,
    NotificationAction,
    NotificationEntity,
    NotificationSeen,
)
from services.db import local_session
from services.logger import root_logger as logger


def query_notifications(author_id: int, after: int = 0) -> Tuple[int, int, List[Tuple[Notification, bool]]]:
    notification_seen_alias = aliased(NotificationSeen)
    q = (
        select(Notification, notification_seen_alias.viewer.label("seen"))
        .outerjoin(
            NotificationSeen,
            and_(
                NotificationSeen.viewer == author_id,
                NotificationSeen.notification == Notification.id,
            ),
        )
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


def group_shout(shout_dict, seen: bool, action: str):
    return {
        "thread": f'shout-{shout_dict.get("id")}',
        "entity": 'shout',
        "shout": shout_dict,
        "authors": shout_dict.get('authors'),
        "updated_at": shout_dict.get('created_at'),
        "reactions": [],
        "action": action,
        "seen": seen
    }


def group_reaction(reaction_dict, seen: bool, action):
    thread_id = reaction_dict['shout']
    if reaction_dict['kind'] == "COMMENT" and reaction_dict.get('reply_to'):
        thread_id += f"shout-{reaction_dict.get('shout')}::{reaction_dict.get('reply_to')}"
    return {
        "thread": thread_id,
        "entity": 'reaction',
        "updated_at": reaction_dict['created_at'],
        "reactions": [reaction_dict['id']],
        "shout": reaction_dict.get('shout'),
        "authors": [reaction_dict.get('created_by'), ],
        "action": action,
        "seen": seen
    }


def group_follower(follower, seen: bool):
    return {
        "thread": "followers",
        "authors": [follower],
        "updated_at": int(time.time()),
        "shout": None,
        "reactions": [],
        "entity": "follower",
        "action": "follow",
        "seen": seen
    }


def get_notifications_grouped(author_id: int, after: int = 0, limit: int = 10):
    """
    Retrieves notifications for a given author.

    Args:
        author_id (int): The ID of the author for whom notifications are retrieved.
        after (int, optional): If provided, selects only notifications created after this timestamp will be considered.
        limit (int, optional): The maximum number of groupa to retrieve.

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
        if groups_amount >= limit:
            break

        payload = notification.payload

        if notification.entity == NotificationEntity.SHOUT.value:
            group = group_shout(payload, seen, notification.action)
            thread_id = group.get('thread')
            groups_by_thread[thread_id] = group
            groups_amount += 1

        elif notification.entity == NotificationEntity.REACTION.value:
            shout_id = payload.get('shout')
            author_id = payload.get('created_by')
            reply_id = payload.get('reply_to')
            thread_id = f'shout-{shout_id}'
            if reply_id and payload.get('kind', '').lower() == 'comment':
                thread_id += f'{reply_id}'
            existing_group = groups_by_thread.get(thread_id)
            if existing_group:
                existing_group['seen'] = False
                existing_group['authors'].append(author_id)
                existing_group['reactions'] = existing_group['reactions'] or []
                existing_group['reactions'].append(payload)
                groups_by_thread[thread_id] = existing_group
            else:
                group = group_reaction(payload, seen, notification.action)  # NOTE: last action will be group-wise
                if group:
                    groups_by_thread[thread_id] = group
                    groups_amount += 1

        elif notification.entity == "follower":
            thread_id = 'followers' if notification.action == 'follow' else 'unfollowers'
            group = groups_by_thread.get(thread_id)
            if group:
                group['authors'].append(payload)
            else:
                group = group_follower(payload, seen)
                groups_amount += 1
            groups_by_thread[thread_id] = group
    return groups_by_thread, unread, total


@query.field('load_notifications')
@login_required
async def load_notifications(_, info, after: int, limit: int = 50):
    author_id = info.context.get("author_id")
    if author_id:
        groups, unread, total = get_notifications_grouped(author_id, after, limit)
        notifications = sorted(groups.values(), key=lambda group: group.updated_at, reverse=True)
        return {"notifications": notifications, "total": total, "unread": unread, "error": None}
    return {"notifications": [], "total": 0, "unread": 0, "error": None}


@mutation.field('notification_mark_seen')
@login_required
async def notification_mark_seen(_, info, notification_id: int):
    author_id = info.context.get('author_id')
    if author_id:
        with local_session() as session:
            try:
                ns = NotificationSeen(notification=notification_id, viewer=author_id)
                session.add(ns)
                session.commit()
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f'seen mutation failed: {e}')
                return {"error": 'cant mark as read'}
    return {"error": None}


@mutation.field('notifications_seen_after')
@login_required
async def notifications_seen_after(_, info, after: int):
    # TODO: use latest loaded notification_id as input offset parameter
    error = None
    try:
        author_id = info.context.get('author_id')
        if author_id:
            with local_session() as session:
                nnn = session.query(Notification).filter(and_(Notification.created_at > after)).all()
                for n in nnn:
                    try:
                        ns = NotificationSeen(notification=n.id, viewer=author_id)
                        session.add(ns)
                        session.commit()
                    except SQLAlchemyError:
                        session.rollback()
    except Exception as e:
        print(e)
        error = 'cant mark as read'
    return {"error": error}


@mutation.field('notifications_seen_thread')
@login_required
async def notifications_seen_thread(_, info, thread: str, after: int):
    error = None
    author_id = info.context.get('author_id')
    if author_id:
        [shout_id, reply_to_id] = thread.split('::')
        with local_session() as session:
            # TODO: handle new follower and new shout notifications
            new_reaction_notifications = (
                session.query(Notification)
                .filter(
                    Notification.action == 'create',
                    Notification.entity == 'reaction',
                    Notification.created_at > after,
                    )
                .all()
            )
            removed_reaction_notifications = (
                session.query(Notification)
                .filter(
                    Notification.action == 'delete',
                    Notification.entity == 'reaction',
                    Notification.created_at > after,
                    )
                .all()
            )
            exclude = set()
            for nr in removed_reaction_notifications:
                reaction = json.loads(nr.payload)
                reaction_id = reaction.get('id')
                exclude.add(reaction_id)
            for n in new_reaction_notifications:
                reaction = json.loads(n.payload)
                reaction_id = reaction.get('id')
                if (
                        reaction_id not in exclude
                        and str(reaction.get('shout')) == str(shout_id)
                        and str(reaction.get('reply_to')) == str(reply_to_id)
                ):
                    try:
                        ns = NotificationSeen(notification=n.id, viewer=author_id)
                        session.add(ns)
                        session.commit()
                    except Exception as e:
                        logger.warn(e)
                        session.rollback()
    else:
        error = 'You are not logged in'
    return {"error": error}
