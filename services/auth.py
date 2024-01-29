import logging
from functools import wraps

from aiohttp import ClientSession
from starlette.exceptions import HTTPException

from settings import AUTH_SECRET, AUTH_URL


logging.basicConfig()
logger = logging.getLogger('\t[services.auth]\t')
logger.setLevel(logging.DEBUG)


async def request_data(gql, headers=None):
    if headers is None:
        headers = {'Content-Type': 'application/json'}
    try:
        # Asynchronous HTTP request to the authentication server
        async with ClientSession() as session:
            async with session.post(AUTH_URL, json=gql, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    errors = data.get('errors')
                    if errors:
                        logger.error(f'[services.auth] errors: {errors}')
                    else:
                        return data
    except Exception as e:
        # Handling and logging exceptions during authentication check
        logger.error(f'[services.auth] request_data error: {e}')
        return None


async def check_auth(req) -> str | None:
    token = req.headers.get('Authorization')
    user_id = ''
    if token:
        # Logging the authentication token
        logger.error(f'[services.auth] checking auth token: {token}')
        query_name = 'validate_jwt_token'
        operation = 'ValidateToken'
        variables = {
            'params': {
                'token_type': 'access_token',
                'token': token,
            }
        }

        gql = {
            'query': f'query {operation}($params: ValidateJWTTokenInput!)  {{' +
                     f'{query_name}(params: $params) {{ is_valid claims }} ' +
                     '}',
            'variables': variables,
            'operationName': operation,
        }
        data = await request_data(gql)
        if data:
            user_id = data.get('data', {}).get(query_name, {}).get('claims', {}).get('sub')
            return user_id

    if not user_id:
        raise HTTPException(status_code=401, detail='Unauthorized')


async def add_user_role(user_id):
    logger.info(f'[services.auth] add author role for user_id: {user_id}')
    query_name = '_update_user'
    operation = 'UpdateUserRoles'
    headers = {
        'Content-Type': 'application/json',
        'x-authorizer-admin-secret': AUTH_SECRET,
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
        user_id = await check_auth(req)
        if user_id:
            context['user_id'] = user_id.strip()
        return await f(*args, **kwargs)

    return decorated_function


def auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        user_id = await check_auth(req)
        if user_id:
            req['user_id'] = user_id.strip()
        return await f(*args, **kwargs)

    return decorated_function
