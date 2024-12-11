from asyncio.log import logger
from ariadne import MutationType, QueryType
import httpx

from settings import AUTH_URL

query = QueryType()
mutation = MutationType()
resolvers = [query, mutation]


async def request_graphql_data(gql, url=AUTH_URL, headers=None):
    if headers is None:
        headers = {"Content-Type": "application/json"}
    try:
        # logger.debug(f"{url}:\n{headers}\n{gql}")
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
        # Handling and logging exceptions during authentication check
        import traceback

        logger.error(f"request_graphql_data error: {traceback.format_exc()}")
    return None
