# Following System

## Overview
System supports following different entity types:
- Authors
- Topics
- Communities
- Shouts (Posts)

## GraphQL API

### Mutations

#### follow
Follow an entity (author/topic/community/shout).

**Parameters:**
- `what: String!` - Entity type (`AUTHOR`, `TOPIC`, `COMMUNITY`, `SHOUT`)
- `slug: String` - Entity slug
- `entity_id: Int` - Optional entity ID

**Returns:**
```typescript
{
  authors?: Author[]        // For AUTHOR type
  topics?: Topic[]          // For TOPIC type
  communities?: Community[] // For COMMUNITY type
  shouts?: Shout[]          // For SHOUT type
  error?: String            // Error message if any
}
```

#### unfollow
Unfollow an entity.

**Parameters:** Same as `follow`

**Returns:** Same as `follow`

### Queries

#### get_shout_followers
Get list of users who reacted to a shout.

**Parameters:**
- `slug: String` - Shout slug
- `shout_id: Int` - Optional shout ID

**Returns:**
```typescript
Author[] // List of authors who reacted
```

## Caching System

### Supported Entity Types
- Authors: `cache_author`, `get_cached_follower_authors`
- Topics: `cache_topic`, `get_cached_follower_topics`
- Communities: No cache
- Shouts: No cache

### Cache Flow
1. On follow/unfollow:
   - Update entity in cache
   - Update follower's following list
2. Cache is updated before notifications

## Notifications

- Sent when author is followed/unfollowed
- Contains:
  - Follower info
  - Author ID
  - Action type ("follow"/"unfollow")

## Error Handling

- Unauthorized access check
- Entity existence validation
- Duplicate follow prevention
- Full error logging
- Transaction safety with `local_session()`

## Database Schema

### Follower Tables
- `AuthorFollower`
- `TopicFollower`
- `CommunityFollower`
- `ShoutReactionsFollower`

Each table contains:
- `follower` - ID of following user
- `{entity_type}` - ID of followed entity 