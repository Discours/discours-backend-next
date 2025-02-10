# GraphQL API Backend

Backend service providing GraphQL API for content management system with reactions, ratings and comments.

## Core Features

### Shouts (Posts)
- CRUD operations via GraphQL mutations
- Rich filtering and sorting options
- Support for multiple authors and topics
- Rating system with likes/dislikes
- Comments and nested replies
- Bookmarks and following

### Reactions System
- `ReactionKind` types: LIKE, DISLIKE, COMMENT
- Rating calculation for shouts and comments
- User-specific reaction tracking
- Reaction stats and aggregations
- Nested comments support

### Authors & Topics
- Author profiles with stats
- Topic categorization and hierarchy
- Following system for authors/topics
- Activity tracking and stats
- Community features

## Tech Stack

- **(Python)[https://www.python.org/]** 3.12+
- **GraphQL** with [Ariadne](https://ariadnegraphql.org/)
- **(SQLAlchemy)[https://docs.sqlalchemy.org/en/20/orm/]**
- **(PostgreSQL)[https://www.postgresql.org/]/(SQLite)[https://www.sqlite.org/]** support
- **(Starlette)[https://www.starlette.io/]** for ASGI server
- **(Redis)[https://redis.io/]** for caching

## Development

### Prepare environment:

```shell
mkdir .venv
python3.12 -m venv venv
source venv/bin/activate
```

### Run server

First, certifcates are required to run the server.

```shell
mkcert -install
mkcert localhost
```

Then, run the server:

```shell
python server.py dev
```

### Useful Commands

```shell
# Linting and import sorting
ruff check . --fix --select I 

# Code formatting
ruff format . --line-length=120 

# Run tests
pytest

# Type checking
mypy .
```

### Code Style

We use:
- Ruff for linting and import sorting
- Line length: 120 characters
- Python type hints
- Docstrings for public methods

### GraphQL Development

Test queries in GraphQL Playground at `http://localhost:8000`:

```graphql
# Example query
query GetShout($slug: String) {
  get_shout(slug: $slug) {
    id
    title
    main_author {
      name
    }
  }
}
```
