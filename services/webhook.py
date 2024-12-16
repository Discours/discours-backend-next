import asyncio
import os
import re
from asyncio.log import logger

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
    """
    Проверяет существование вебхука для user.login события
    
    Returns:
        tuple: (bool, str, str) - существует ли вебхук, его id и endpoint если существует
    """
    logger.info("check_webhook_existence called")
    if not ADMIN_SECRET:
        logger.error("ADMIN_SECRET is not set")
        return False, None, None
    
    headers = {
        "Content-Type": "application/json",
        "X-Authorizer-Admin-Secret": ADMIN_SECRET
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
        webhooks = result.get("data", {}).get(query_name, {}).get("webhooks", [])
        logger.info(webhooks)
        for webhook in webhooks:
            if webhook["event_name"].startswith("user.login"):
                return True, webhook["id"], webhook["endpoint"]
    return False, None, None


async def create_webhook_endpoint():
    """
    Создает вебхук для user.login события.
    Если существует старый вебхук - удаляет его и создает новый.
    """
    logger.info("create_webhook_endpoint called")

    headers = {
        "Content-Type": "application/json",
        "X-Authorizer-Admin-Secret": ADMIN_SECRET
    }

    exists, webhook_id, current_endpoint = await check_webhook_existence()
    
    # Определяем endpoint в зависимости от окружения
    host = os.environ.get('HOST', 'core.dscrs.site')
    endpoint = f"https://{host}/new-author"
    
    if exists:
        # Если вебхук существует, но с другим endpoint или с модифицированным именем
        if current_endpoint != endpoint or webhook_id:
            # https://docs.authorizer.dev/core/graphql-api#_delete_webhook
            operation = "DeleteWebhook"
            query_name = "_delete_webhook"
            variables = {"params": {"id": webhook_id}}  # Изменено с id на webhook_id
            gql = {
                "query": f"mutation {operation}($params: WebhookRequest!)"
                + "{"
                + f"{query_name}(params: $params) {{ message }} "
                + "}",
                "variables": variables,
                "operationName": operation,
            }
            try:
                await request_graphql_data(gql, headers=headers)
                exists = False
            except Exception as e:
                logger.error(f"Failed to delete webhook: {e}")
                # Продолжаем выполнение даже при ошибке удаления
                exists = False
        else:
            logger.info(f"Webhook already exists and configured correctly: {webhook_id}")
            return

    if not exists:
        # https://docs.authorizer.dev/core/graphql-api#_add_webhook
        operation = "AddWebhook"
        query_name = "_add_webhook"
        variables = {
            "params": {
                "event_name": "user.login",
                "endpoint": endpoint,
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
        try:
            result = await request_graphql_data(gql, headers=headers)
            logger.info(result)
        except Exception as e:
            logger.error(f"Failed to create webhook: {e}")


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
