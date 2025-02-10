from asyncio.log import logger

import httpx
from ariadne import MutationType, QueryType

from services.db import create_table_if_not_exists, local_session
from settings import AUTH_URL

query = QueryType()
mutation = MutationType()
resolvers = [query, mutation]


async def request_graphql_data(gql, url=AUTH_URL, headers=None):
    """
    Выполняет GraphQL запрос к указанному URL

    :param gql: GraphQL запрос
    :param url: URL для запроса, по умолчанию AUTH_URL
    :param headers: Заголовки запроса
    :return: Результат запроса или None в случае ошибки
    """
    if not url:
        return None
    if headers is None:
        headers = {"Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=gql, headers=headers)
            if response.status_code == 200:
                data = response.json()
                errors = data.get("errors")
                if errors:
                    logger.error(f"{url} response: {data}")
                else:
                    return data
            else:
                logger.error(f"{url}: {response.status_code} {response.text}")
    except Exception as _e:
        import traceback

        logger.error(f"request_graphql_data error: {traceback.format_exc()}")
    return None



def create_all_tables():
    """Create all database tables in the correct order."""
    from orm import author, community, draft, notification, reaction, shout, topic, user

    # Порядок важен - сначала таблицы без внешних ключей, затем зависимые таблицы
    models_in_order = [
        user.User,  # Базовая таблица auth
        author.Author,  # Базовая таблица
        community.Community,  # Базовая таблица
        topic.Topic,  # Базовая таблица
        
        # Связи для базовых таблиц
        author.AuthorFollower,  # Зависит от Author
        community.CommunityFollower,  # Зависит от Community
        topic.TopicFollower,  # Зависит от Topic
        
        # Черновики (теперь без зависимости от Shout)
        draft.Draft,  # Зависит только от Author
        draft.DraftAuthor,  # Зависит от Draft и Author
        draft.DraftTopic,  # Зависит от Draft и Topic
        
        # Основные таблицы контента
        shout.Shout,  # Зависит от Author и Draft
        shout.ShoutAuthor,  # Зависит от Shout и Author
        shout.ShoutTopic,  # Зависит от Shout и Topic
        
        # Реакции
        reaction.Reaction,  # Зависит от Author и Shout
        shout.ShoutReactionsFollower,  # Зависит от Shout и Reaction
        
        # Дополнительные таблицы
        author.AuthorRating,  # Зависит от Author
        notification.Notification,  # Зависит от Author
        notification.NotificationSeen,  # Зависит от Notification
        # collection.Collection,
        # collection.ShoutCollection,
        # invite.Invite
    ]

    with local_session() as session:
        for model in models_in_order:
            try:
                create_table_if_not_exists(session.get_bind(), model)
                logger.info(f"Created or verified table: {model.__tablename__}")
            except Exception as e:
                logger.error(f"Error creating table {model.__tablename__}: {e}")
                raise