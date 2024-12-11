import asyncio
from asyncio.log import logger
import os
import re

from sqlalchemy import select
from starlette.endpoints import HTTPEndpoint
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from cache.cache import cache_author
from orm.author import Author
from resolvers.stat import get_with_stat
from services.db import local_session
from services.schema import request_graphql_data
from settings import ADMIN_SECRET, WEBHOOK_SECRET


async def check_webhook_existence():
    logger.info("check_webhook_existence called")

    headers = {
        "Content-Type": "application/json",
        "x-authorizer-admin-secret": ADMIN_SECRET,
    }

    operation = "GetWebhooks"
    query_name = "_webhooks"
    variables = {"params": {}}
    # https://docs.authorizer.dev/core/graphql-api#_webhooks
    gql = {
        "query": f"query {operation}($params: PaginatedInput!)"
        + "{"
        + f"{query_name}(params: $params) {{ webhooks {{ id event_name endpoint }} }} "
        + "}",
        "variables": variables,
        "operationName": operation,
    }
    result = await request_graphql_data(gql, headers=headers)
    if result:
        logger.info(result)
        return bool(result.get("data", {}).get(query_name, {}).get("webhooks", []))
    return False


async def create_webhook_endpoint():
    logger.info("create_webhook_endpoint called")

    headers = {
        "Content-Type": "application/json",
        "x-authorizer-admin-secret": ADMIN_SECRET,
    }

    if await check_webhook_existence():
        logger.info("Webhook already exists")
        return

    # https://docs.authorizer.dev/core/graphql-api#_add_webhook
    operation = "AddWebhook"
    query_name = "_add_webhook"
    variables = {
        "params": {
            "event_name": "user.login",
            "endpoint": "https://core.dscrs.site/new-author",
            "enabled": True,
            "headers": {"Authorization": WEBHOOK_SECRET},
        }
    }
    gql = {
        "query": f"mutation {operation}($params: AddWebhookRequest!)"
        + "{"
        + f"{query_name}(params: $params) {{ message }} "
        + "}",
        "variables": variables,
        "operationName": operation,
    }
    result = await request_graphql_data(gql, headers=headers)
    logger.info(result)


class WebhookEndpoint(HTTPEndpoint):
    async def post(self, request: Request) -> JSONResponse:
        try:
            data = await request.json()
            if not data:
                raise HTTPException(status_code=400, detail="Request body is empty")
            auth = request.headers.get("Authorization")
            if not auth or auth != os.environ.get("WEBHOOK_SECRET"):
                raise HTTPException(status_code=401, detail="Invalid Authorization header")
            # logger.debug(data)
            user = data.get("user")
            if not isinstance(user, dict):
                raise HTTPException(status_code=400, detail="User data is not a dictionary")
            #
            name: str = (
                f"{user.get('given_name', user.get('slug'))} {user.get('middle_name', '')}"
                + f"{user.get('family_name', '')}".strip()
            ) or "Аноним"
            user_id: str = user.get("id", "")
            email: str = user.get("email", "")
            pic: str = user.get("picture", "")
            if user_id:
                with local_session() as session:
                    author = session.query(Author).filter(Author.user == user_id).first()
                    if not author:
                        # If the author does not exist, create a new one
                        slug: str = email.split("@")[0].replace(".", "-").lower()
                        slug: str = re.sub("[^0-9a-z]+", "-", slug)
                        while True:
                            author = session.query(Author).filter(Author.slug == slug).first()
                            if not author:
                                break
                            slug = f"{slug}-{len(session.query(Author).filter(Author.email == email).all()) + 1}"
                        author = Author(user=user_id, slug=slug, name=name, pic=pic)
                        session.add(author)
                        session.commit()
                        author_query = select(Author).filter(Author.user == user_id)
                        result = get_with_stat(author_query)
                        if result:
                            author_with_stat = result[0]
                            author_dict = author_with_stat.dict()
                            # await cache_author(author_with_stat)
                            asyncio.create_task(cache_author(author_dict))

            return JSONResponse({"status": "success"})
        except HTTPException as e:
            return JSONResponse({"status": "error", "message": str(e.detail)}, status_code=e.status_code)
        except Exception as e:
            import traceback

            traceback.print_exc()
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
