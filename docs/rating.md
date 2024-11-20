# Rating System

## GraphQL Resolvers

### Queries

#### get_my_rates_shouts
Get user's reactions (LIKE/DISLIKE) for specified posts.

**Parameters:**
- `shouts: [Int!]!` - array of shout IDs

**Returns:**
```typescript
[{
  shout_id: Int
  my_rate: ReactionKind // LIKE or DISLIKE
}]
```

#### get_my_rates_comments
Get user's reactions (LIKE/DISLIKE) for specified comments.

**Parameters:**
- `comments: [Int!]!` - array of comment IDs

**Returns:**
```typescript
[{
  comment_id: Int
  my_rate: ReactionKind // LIKE or DISLIKE
}]
```

### Mutations

#### rate_author
Rate another author (karma system).

**Parameters:**
- `rated_slug: String!` - author's slug
- `value: Int!` - rating value (positive/negative)

## Rating Calculation

### Author Rating Components

#### Shouts Rating
- Calculated from LIKE/DISLIKE reactions on author's posts
- Each LIKE: +1
- Each DISLIKE: -1
- Excludes deleted reactions
- Excludes comment reactions

#### Comments Rating  
- Calculated from LIKE/DISLIKE reactions on author's comments
- Each LIKE: +1
- Each DISLIKE: -1
- Only counts reactions to COMMENT type reactions
- Excludes deleted reactions

#### Legacy Karma
- Based on direct author ratings via `rate_author` mutation
- Stored in `AuthorRating` table
- Each positive rating: +1
- Each negative rating: -1

### Helper Functions

- `count_author_comments_rating()` - Calculate comment rating
- `count_author_shouts_rating()` - Calculate posts rating
- `get_author_rating_old()` - Get legacy karma rating
- `get_author_rating_shouts()` - Get posts rating (optimized)
- `get_author_rating_comments()` - Get comments rating (optimized)
- `add_author_rating_columns()` - Add rating columns to author query

## Notes

- All ratings exclude deleted content
- Reactions are unique per user/content
- Rating calculations are optimized with SQLAlchemy
- System supports both direct author rating and content-based rating 