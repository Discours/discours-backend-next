from cache.triggers import events_register
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
from resolvers.draft import (
    create_draft,
    delete_draft,
    load_drafts,
    publish_draft,
    unpublish_draft,
    update_draft,
)

from resolvers.feed import (
    load_shouts_coauthored,
    load_shouts_discussed,
    load_shouts_feed,
    load_shouts_followed_by,
)
from resolvers.follower import follow, get_shout_followers, unfollow
from resolvers.notifier import (
    load_notifications,
    notification_mark_seen,
    notifications_seen_after,
    notifications_seen_thread,
)
from resolvers.rating import get_my_rates_comments, get_my_rates_shouts, rate_author
from resolvers.reaction import (
    create_reaction,
    delete_reaction,
    load_comment_ratings,
    load_reactions_by,
    load_shout_comments,
    load_shout_ratings,
    update_reaction,
)
from resolvers.reader import (
    get_shout,
    load_shouts_by,
    load_shouts_random_top,
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
    "load_shouts_random_top",
    "load_shouts_search",
    "load_shouts_unrated",
    # feed
    "load_shouts_feed",
    "load_shouts_coauthored",
    "load_shouts_discussed",
    "load_shouts_with_topic",
    "load_shouts_followed_by",
    "load_shouts_authored_by",
    # follower
    "follow",
    "unfollow",
    "get_shout_followers",
    # reaction
    "create_reaction",
    "update_reaction",
    "delete_reaction",
    "load_reactions_by",
    "load_shout_comments",
    "load_shout_ratings",
    "load_comment_ratings",
    # notifier
    "load_notifications",
    "notifications_seen_thread",
    "notifications_seen_after",
    "notification_mark_seen",
    # rating
    "rate_author",
    "get_my_rates_comments",
    "get_my_rates_shouts",
    # draft
    "load_drafts",
    "create_draft",
    "update_draft",
    "delete_draft",
    "publish_draft",
    "publish_shout",
    "unpublish_shout",
    "unpublish_draft",
]
