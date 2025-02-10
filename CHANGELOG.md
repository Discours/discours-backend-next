#### [0.4.9] - 2025-02-09
- `Shout.draft` field added
- `Draft` entity added
- `create_draft`, `update_draft`, `delete_draft` mutations and resolvers added
- `create_shout`, `update_shout`, `delete_shout` mutations removed from GraphQL API
- `load_drafts` resolver implemented
- `publish_` and `unpublish_` mutations and resolvers added
- `create_`, `update_`, `delete_` mutations and resolvers added for `Draft` entity
- tests with pytest for original auth, shouts, drafts

#### [0.4.8] - 2025-02-03
- `Reaction.deleted_at` filter on `update_reaction` resolver added
- `triggers` module updated with `after_shout_handler`, `after_reaction_handler` for cache revalidation
- `after_shout_handler`, `after_reaction_handler` now also handle `deleted_at` field
- `get_cached_topic_followers` fixed
- `get_my_rates_comments` fixed

#### [0.4.7]
- `get_my_rates_shouts` resolver added with:
  - `shout_id` and `my_rate` fields in response
  - filters by `Reaction.deleted_at.is_(None)`
  - filters by `Reaction.kind.in_([ReactionKind.LIKE.value, ReactionKind.DISLIKE.value])`
  - filters by `Reaction.reply_to.is_(None)`
  - uses `local_session()` context manager
  - returns empty list on errors
- SQLAlchemy syntax updated:
  - `select()` statement fixed for newer versions
  - `Reaction` model direct selection instead of labeled columns
  - proper row access with `row[0].shout` and `row[0].kind`
- GraphQL resolver fixes:
  - added root parameter `_` to match schema
  - proper async/await handling with `@login_required`
  - error logging added via `logger.error()`

#### [0.4.6]
- login_accepted decorator added
- `docs` added
- optimized and unified `load_shouts_*` resolvers with `LoadShoutsOptions`
- `load_shouts_bookmarked` resolver fixed
- resolvers updates:
    - new resolvers group `feed`
    - `load_shouts_authored_by` resolver added
    - `load_shouts_with_topic` resolver added
    - `load_shouts_followed` removed
    - `load_shouts_random_topic` removed
    - `get_topics_random` removed
- model updates:
    - `ShoutsOrderBy` enum added
    - `Shout.main_topic` from `ShoutTopic.main` as `Topic` type output
    - `Shout.created_by` as `Author` type output

#### [0.4.5]
- `bookmark_shout` mutation resolver added
- `load_shouts_bookmarked` resolver added
- `get_communities_by_author` resolver added
- `get_communities_all` resolver fixed
- `Community` stats in orm
- `Community` CUDL resolvers added
- `Reaction` filter by `Reaction.kind`s
- `ReactionSort` enum added
- `CommunityFollowerRole` enum added
- `InviteStatus` enum added
- `Topic.parents` ids added
- `get_shout` resolver accepts slug or shout_id

#### [0.4.4]
- `followers_stat` removed for shout
- sqlite3 support added
- `rating_stat` and `commented_stat` fixes

#### [0.4.3]
- cache reimplemented
- load shouts queries unified
- `followers_stat` removed from shout

#### [0.4.2]
- reactions load resolvers separated for ratings (no stats) and comments
- reactions stats improved
- `load_comment_ratings` separate resolver

#### [0.4.1]
- follow/unfollow logic updated and unified with cache

#### [0.4.0]
- chore: version migrator synced
- feat: precache_data on start
- fix: store id list for following cache data
- fix: shouts stat filter out deleted

#### [0.3.5]
- cache isolated to services
- topics followers and authors cached
- redis stores lists of ids

#### [0.3.4]
- `load_authors_by` from cache

#### [0.3.3]
- feat: sentry integration enabled with glitchtip
- fix: reindex on update shout
- packages upgrade, isort
- separated stats queries for author and topic
- fix: feed featured filter
- fts search removed

#### [0.3.2]
- redis cache for what author follows
- redis cache for followers
- graphql add query: get topic followers

#### [0.3.1]
- enabling sentry
- long query log report added
- editor fixes
- authors links cannot be updated by `update_shout` anymore

#### [0.3.0]
- `Shout.featured_at` timestamp of the frontpage featuring event
- added proposal accepting logics
- schema modulized
- Shout.visibility removed

#### [0.2.22]
- added precommit hook
- fmt
- granian asgi

#### [0.2.21]
- fix: rating logix
- fix: `load_top_random_shouts`
- resolvers: `add_stat_*` refactored
- services: use google analytics
- services: minor fixes search

#### [0.2.20]
- services: ackee removed
- services: following manager fixed
- services: import views.json

#### [0.2.19]
- fix: adding `author` role
- fix: stripping `user_id` in auth connector

#### [0.2.18]
- schema: added `Shout.seo` string field
- resolvers: added `/new-author` webhook resolver
- resolvers: added reader.load_shouts_top_random
- resolvers: added reader.load_shouts_unrated
- resolvers: community follower id property name is `.author`
- resolvers: `get_authors_all` and `load_authors_by`
- services: auth connector upgraded

#### [0.2.17]
- schema: enum types workaround, `ReactionKind`, `InviteStatus`, `ShoutVisibility`
- schema: `Shout.created_by`, `Shout.updated_by`
- schema: `Shout.authors` can be empty
- resolvers: optimized `reacted_shouts_updates` query

#### [0.2.16]
- resolvers: collab inviting logics
- resolvers: queries and mutations revision and renaming
- resolvers: `delete_topic(slug)` implemented
- resolvers: added `get_shout_followers`
- resolvers: `load_shouts_by` filters implemented
- orm: invite entity
- schema: `Reaction.range` -> `Reaction.quote`
- filters: `time_ago` -> `after`
- httpx -> aiohttp

#### [0.2.15]
- schema: `Shout.created_by` removed
- schema: `Shout.mainTopic` removed
- services: cached elasticsearch connector
- services: auth is using `user_id` from authorizer
- resolvers: `notify_*` usage fixes
- resolvers: `getAuthor` now accepts slug, `user_id` or `author_id`
- resolvers: login_required usage fixes

#### [0.2.14]
- schema: some fixes from migrator
- schema: `.days` -> `.time_ago`
- schema: `excludeLayout` + `layout` in filters -> `layouts`
- services: db access simpler, no contextmanager
- services: removed Base.create() method
- services: rediscache updated
- resolvers: get_reacted_shouts_updates as followedReactions query

#### [0.2.13]
- services: db context manager
- services: `ViewedStorage` fixes
- services: views are not stored in core db anymore
- schema: snake case in model fields names
- schema: no DateTime scalar
- resolvers: `get_my_feed` comments filter reactions body.is_not('')
- resolvers: `get_my_feed` query fix
- resolvers: `LoadReactionsBy.days` -> `LoadReactionsBy.time_ago`
- resolvers: `LoadShoutsBy.days` -> `LoadShoutsBy.time_ago`

#### [0.2.12]
- `Author.userpic` -> `Author.pic`
- `CommunityFollower.role` is string now
- `Author.user` is string now

#### [0.2.11]
- redis interface updated
- `viewed` interface updated
- `presence` interface updated
- notify on create, update, delete for reaction and shout
- notify on follow / unfollow author
- use pyproject
- devmode fixed

#### [0.2.10]
- community resolvers connected

#### [0.2.9]
- starlette is back, aiohttp removed
- aioredis replaced with aredis

#### [0.2.8]
- refactored


#### [0.2.7]
- `loadFollowedReactions` now with `login_required`
- notifier service api draft
- added `shout` visibility kind in schema
- community isolated from author in orm


#### [0.2.6]
- redis connection pool
- auth context fixes
- communities orm, resolvers, schema


#### [0.2.5]
- restructured
- all users have their profiles as authors in core
- `gittask`, `inbox` and `auth` logics removed
- `settings` moved to base and now smaller
- new outside auth schema
- removed `gittask`, `auth`, `inbox`, `migration`
