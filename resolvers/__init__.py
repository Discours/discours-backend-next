from resolvers.author import (  # search_authors,
    get_author,
    get_author_followers,
    get_author_follows,
    get_author_follows_authors,
    get_author_follows_topics,
    get_author_id,
    get_authors_all,
    load_authors_by,
    update_author,
)
from resolvers.community import get_communities_all, get_community
from resolvers.editor import create_shout, delete_shout, update_shout
from resolvers.follower import follow, get_shout_followers, unfollow
from resolvers.notifier import (
    load_notifications,
    notification_mark_seen,
    notifications_seen_after,
    notifications_seen_thread,
)
from resolvers.rating import rate_author
from resolvers.reaction import (
    create_reaction,
    delete_reaction,
    load_reactions_by,
    load_shouts_followed,
    update_reaction,
)
from resolvers.reader import (
    get_shout,
    load_shouts_by,
    load_shouts_feed,
    load_shouts_random_top,
    load_shouts_random_topic,
    load_shouts_search,
    load_shouts_unrated,
)
from resolvers.topic import (
    get_topic,
    get_topic_authors,
    get_topic_followers,
    get_topics_all,
    get_topics_by_author,
    get_topics_by_community,
)
from services.triggers import events_register

events_register()

__all__ = [
    # author
    "get_author",
    "get_author_id",
    "get_author_followers",
    "get_author_follows",
    "get_author_follows_topics",
    "get_author_follows_authors",
    "get_authors_all",
    "load_authors_by",
    "rate_author",
    "update_author",
    ## "search_authors",
    # community
    "get_community",
    "get_communities_all",
    # topic
    "get_topic",
    "get_topics_all",
    "get_topics_by_community",
    "get_topics_by_author",
    "get_topic_followers",
    "get_topic_authors",
    # reader
    "get_shout",
    "load_shouts_by",
    "load_shouts_feed",
    "load_shouts_search",
    "load_shouts_followed",
    "load_shouts_unrated",
    "load_shouts_random_top",
    "load_shouts_random_topic",
    # follower
    "follow",
    "unfollow",
    "get_shout_followers",
    # editor
    "create_shout",
    "update_shout",
    "delete_shout",
    # reaction
    "create_reaction",
    "update_reaction",
    "delete_reaction",
    "load_reactions_by",
    # notifier
    "load_notifications",
    "notifications_seen_thread",
    "notifications_seen_after",
    "notification_mark_seen",
]
