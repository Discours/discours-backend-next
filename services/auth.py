from functools import wraps

from cache.cache import get_cached_author_by_user_id
from resolvers.stat import get_with_stat
from services.schema import request_graphql_data
from settings import ADMIN_SECRET, AUTH_URL
from utils.logger import root_logger as logger


async def check_auth(req):
    """
    Проверка авторизации пользователя.

    Эта функция проверяет токен авторизации, переданный в заголовках запроса,
    и возвращает идентификатор пользователя и его роли.

    Параметры:
    - req: Входящий GraphQL запрос, содержащий заголовок авторизации.

    Возвращает:
    - user_id: str - Идентификатор пользователя.
    - user_roles: list[str] - Список ролей пользователя.
    """
    token = req.headers.get("Authorization")
    host = req.headers.get('host', '')
    logger.debug(f"check_auth: host={host}")
    auth_url = AUTH_URL
    if host == 'testing.dscrs.site' or host == 'localhost':
        auth_url = "https://auth.dscrs.site/graphql"
    user_id = ""
    user_roles = []
    if token:
        # Logging the authentication token
        logger.debug(f"{token}")
        query_name = "validate_jwt_token"
        operation = "ValidateToken"
        variables = {"params": {"token_type": "access_token", "token": token}}

        gql = {
            "query": f"query {operation}($params: ValidateJWTTokenInput!)"
            + "{"
            + f"{query_name}(params: $params) {{ is_valid claims }} "
            + "}",
            "variables": variables,
            "operationName": operation,
        }
        data = await request_graphql_data(gql, url=auth_url)
        if data:
            logger.debug(data)
            user_data = data.get("data", {}).get(query_name, {}).get("claims", {})
            user_id = user_data.get("sub", "")
            user_roles = user_data.get("allowed_roles", [])
    return user_id, user_roles


async def add_user_role(user_id):
    """
    Добавление роли пользователя.

    Эта функция добавляет роли "author" и "reader" для указанного пользователя
    в системе авторизации.

    Параметры:
    - user_id: str - Идентификатор пользователя, которому нужно добавить роли.

    Возвращает:
    - user_id: str - Идентификатор пользователя, если операция прошла успешно.
    """
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
    data = await request_graphql_data(gql, headers=headers)
    if data:
        user_id = data.get("data", {}).get(query_name, {}).get("id")
        return user_id


def login_required(f):
    """
    Декоратор для проверки авторизации пользователя.

    Этот декоратор проверяет, авторизован ли пользователь, и добавляет
    информацию о пользователе в контекст функции.

    Параметры:
    - f: Функция, которую нужно декорировать.

    Возвращает:
    - Обернутую функцию с добавленной проверкой авторизации.
    """

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
    Декоратор для добавления данных авторизации в контекст.

    Этот декоратор добавляет данные авторизации в контекст, если они доступны,
    но не блокирует доступ для неавторизованных пользователей.

    Параметры:
    - f: Функция, которую нужно декорировать.

    Возвращает:
    - Обернутую функцию с добавленной проверкой авторизации.
    """

    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        req = info.context.get("request")

        logger.debug("login_accepted: Проверка авторизации пользователя.")
        user_id, user_roles = await check_auth(req)
        logger.debug(f"login_accepted: user_id={user_id}, user_roles={user_roles}")

        if user_id and user_roles:
            logger.info(f"login_accepted: Пользователь авторизован: {user_id} с ролями {user_roles}")
            info.context["user_id"] = user_id.strip()
            info.context["roles"] = user_roles

            # Пробуем получить профиль автора
            author = await get_cached_author_by_user_id(user_id, get_with_stat)
            if author:
                logger.debug(f"login_accepted: Найден профиль автора: {author}")
                # Предполагается, что `author` является объектом с атрибутом `id`
                info.context["author"] = author.dict()
            else:
                logger.error(
                    f"login_accepted: Профиль автора не найден для пользователя {user_id}. Исп��льзуем базовые данные."
                )  # Используем базовую информацию об автор
        else:
            logger.debug("login_accepted: Пользователь не авторизован. Очищаем контекст.")
            info.context["user_id"] = None
            info.context["roles"] = None
            info.context["author"] = None

        return await f(*args, **kwargs)

    return decorated_function
