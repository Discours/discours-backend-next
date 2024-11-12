from functools import wraps

import httpx

from cache.cache import get_cached_author_by_user_id
from resolvers.stat import get_with_stat
from settings import ADMIN_SECRET, AUTH_URL
from utils.logger import root_logger as logger


async def request_data(gql, headers=None):
    if headers is None:
        headers = {"Content-Type": "application/json"}
    try:
        # logger.debug(f"{AUTH_URL} {gql} {headers}")
        async with httpx.AsyncClient() as client:
            response = await client.post(AUTH_URL, json=gql, headers=headers)
            if response.status_code == 200:
                data = response.json()
                errors = data.get("errors")
                if errors:
                    logger.error(f"{AUTH_URL} response: {data}")
                else:
                    return data
    except Exception as _e:
        # Handling and logging exceptions during authentication check
        import traceback

        logger.error(f"request_data error: {traceback.format_exc()}")
    return None


async def check_auth(req):
    token = req.headers.get("Authorization")
    user_id = ""
    user_roles = []
    if token:
        # Logging the authentication token
        logger.debug(f"{token}")
        query_name = "validate_jwt_token"
        operation = "ValidateToken"
        variables = {"params": {"token_type": "access_token", "token": token}}

        gql = {
            "query": f"query {operation}($params: ValidateJWTTokenInput!)  {{"
            + f"{query_name}(params: $params) {{ is_valid claims }} "
            + "}",
            "variables": variables,
            "operationName": operation,
        }
        data = await request_data(gql)
        if data:
            logger.debug(data)
            user_data = data.get("data", {}).get(query_name, {}).get("claims", {})
            user_id = user_data.get("sub", "")
            user_roles = user_data.get("allowed_roles", [])
    return user_id, user_roles


async def add_user_role(user_id):
    logger.info(f"add author role for user_id: {user_id}")
    query_name = "_update_user"
    operation = "UpdateUserRoles"
    headers = {
        "Content-Type": "application/json",
        "x-authorizer-admin-secret": ADMIN_SECRET,
    }
    variables = {"params": {"roles": "author, reader", "id": user_id}}
    gql = {
        "query": f"mutation {operation}($params: UpdateUserInput!) {{ {query_name}(params: $params) {{ id roles }} }}",
        "variables": variables,
        "operationName": operation,
    }
    data = await request_data(gql, headers)
    if data:
        user_id = data.get("data", {}).get(query_name, {}).get("id")
        return user_id


def login_required(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        req = info.context.get("request")
        user_id, user_roles = await check_auth(req)
        if user_id and user_roles:
            logger.info(f" got {user_id} roles: {user_roles}")
            info.context["user_id"] = user_id.strip()
            info.context["roles"] = user_roles
            author = await get_cached_author_by_user_id(user_id, get_with_stat)
            if not author:
                logger.error(f"author profile not found for user {user_id}")
            info.context["author"] = author
        return await f(*args, **kwargs)

    return decorated_function


def login_accepted(f):
    """
    Декоратор, который добавляет данные авторизации в контекст, если они доступны,
    но не блокирует доступ для неавторизованных пользователей.
    """

    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        req = info.context.get("request")

        # Пробуем получить данные авторизации
        user_id, user_roles = await check_auth(req)

        if user_id and user_roles:
            # Если пользователь авторизован, добавляем его данные в контекст
            logger.info(f" got {user_id} roles: {user_roles}")
            info.context["user_id"] = user_id.strip()
            info.context["roles"] = user_roles

            # Пробуем получить профиль автора
            author = await get_cached_author_by_user_id(user_id, get_with_stat)
            if not author:
                logger.warning(f"author profile not found for user {user_id}")
            info.context["author"] = author
        else:
            # Для неавторизованных пользователей очищаем контекст
            info.context["user_id"] = None
            info.context["roles"] = None
            info.context["author"] = None

        return await f(*args, **kwargs)

    return decorated_function
