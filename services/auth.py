import logging
from functools import wraps

import httpx
from dogpile.cache import make_region

from settings import ADMIN_SECRET, AUTH_URL

logger = logging.getLogger('\t[services.auth]\t')
logger.setLevel(logging.DEBUG)


async def request_data(gql, headers=None):
    if headers is None:
        headers = {'Content-Type': 'application/json'}
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(AUTH_URL, json=gql, headers=headers)
            if response.status_code == 200:
                data = response.json()
                errors = data.get('errors')
                if errors:
                    logger.error(f'HTTP Errors: {errors}')
                else:
                    return data
    except Exception as e:
        # Handling and logging exceptions during authentication check
        logger.error(f'request_data error: {e}')
        return None


# Создание региона кэша с TTL 30 секунд
region = make_region().configure('dogpile.cache.memory', expiration_time=30)

# Функция-ключ для кэширования
def auth_cache_key(req):
    token = req.headers.get('Authorization')
    return f"auth_token:{token}"

# Декоратор для кэширования запроса проверки токена
def cache_auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        cache_key = auth_cache_key(req)
        result = region.get(cache_key)
        if result is None:
            [user_id, user_roles]  = await f(*args, **kwargs)
            if user_id:
                region.set(cache_key, [user_id, user_roles])
        return result
    return decorated_function

# Измененная функция проверки аутентификации с кэшированием
@cache_auth_request
async def check_auth(req):
    token = req.headers.get('Authorization')
    user_id = ''
    user_roles = []

    if token:
        # Logging the authentication token
        logger.debug(f'{token}')
        query_name = 'validate_jwt_token'
        operation = 'ValidateToken'
        variables = {
            'params': {
                'token_type': 'access_token',
                'token': token,
            }
        }

        gql = {
            'query': f'query {operation}($params: ValidateJWTTokenInput!)  {{ {query_name}(params: $params) {{ is_valid claims }} }}',
            'variables': variables,
            'operationName': operation,
        }
        data = await request_data(gql)
        if data:
            user_data = data.get('data', {}).get(query_name, {}).get('claims', {})
            user_id = user_data.get('sub')
            user_roles = user_data.get('allowed_roles')

    # Возвращаем пустые значения, если не удалось получить user_id и user_roles
    return [user_id, user_roles]


async def add_user_role(user_id):
    logger.info(f'[services.auth] add author role for user_id: {user_id}')
    query_name = '_update_user'
    operation = 'UpdateUserRoles'
    headers = {
        'Content-Type': 'application/json',
        'x-authorizer-admin-secret': ADMIN_SECRET,
    }
    variables = {'params': {'roles': 'author, reader', 'id': user_id}}
    gql = {
        'query': f'mutation {operation}($params: UpdateUserInput!) {{ {query_name}(params: $params) {{ id roles }} }}',
        'variables': variables,
        'operationName': operation,
    }
    data = await request_data(gql, headers)
    if data:
        user_id = data.get('data', {}).get(query_name, {}).get('id')
        return user_id


def login_required(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        context = info.context
        req = context.get('request')
        # Проверяем, есть ли значения в кэше, и используем их, если они есть
        [user_id, user_roles] = await check_auth(req)
        if user_id and user_roles:
            logger.info(f' got {user_id} roles: {user_roles}')
            context['user_id'] = user_id.strip()
            context['roles'] = user_roles
        return await f(*args, **kwargs)

    return decorated_function


def auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        # Проверяем, есть ли значения в кэше, и используем их, если они есть
        [user_id, user_roles] = await check_auth(req)
        if user_id:
            req['user_id'] = user_id.strip()
            req['roles'] = user_roles
        return await f(*args, **kwargs)

    return decorated_function
