from cachetools import TTLCache, cached
import logging
import time
from starlette.exceptions import HTTPException

from aiohttp import ClientSession
from settings import AUTH_URL, AUTH_SECRET


logging.basicConfig()
logger = logging.getLogger("\t[services.auth]\t")
logger.setLevel(logging.DEBUG)

# Define a TTLCache with a time-to-live of 100 seconds
token_cache = TTLCache(maxsize=99999, ttl=1799)

async def request_data(gql, headers={"Content-Type": "application/json"}):
    try:
        async with ClientSession() as session:
            async with session.post(AUTH_URL, json=gql, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    errors = data.get("errors")
                    if errors:
                        logger.error(f"[services.auth] errors: {errors}")
                    else:
                        return data
    except Exception as e:
        logger.error(f"[services.auth] request_data error: {e}")
        return None

@cached(cache=token_cache)
async def user_id_from_token(token):
    logger.error(f"[services.auth] checking auth token: {token}")
    query_name = "validate_jwt_token"
    operation = "ValidateToken"
    variables = {
        "params": {
            "token_type": "access_token",
            "token": token,
        }
    }

    gql = {
        "query": f"query {operation}($params: ValidateJWTTokenInput!) {{ {query_name}(params: $params) {{ is_valid claims }} }}",
        "variables": variables,
        "operationName": operation,
    }
    data = await request_data(gql)
    if data:
        expires_in = data.get("data", {}).get(query_name, {}).get("claims", {}).get("expires_in")
        user_id = data.get("data", {}).get(query_name, {}).get("claims", {}).get("sub")
        if expires_in is not None and user_id is not None:
            if expires_in < 100:
                # Token will expire soon, remove it from cache
                token_cache.pop(token, None)
            else:
                expires_at = time.time() + expires_in
                return user_id, expires_at

async def check_auth(req) -> str | None:
    token = req.headers.get("Authorization")
    cached_result = await user_id_from_token(token)
    if cached_result:
        user_id, expires_at = cached_result
        if expires_at > time.time():
            return user_id
    raise HTTPException(status_code=401, detail="Unauthorized")

async def add_user_role(user_id):
    logger.info(f"[services.auth] add author role for user_id: {user_id}")
    query_name = "_update_user"
    operation = "UpdateUserRoles"
    headers = {"Content-Type": "application/json", "x-authorizer-admin-secret": AUTH_SECRET}
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
    async def decorated_function(*args, **kwargs):
        info = args[1]
        context = info.context
        req = context.get("request")
        user_id = await check_auth(req)
        if user_id:
            context["user_id"] = user_id.strip()
        return await f(*args, **kwargs)

    return decorated_function

def auth_request(f):
    async def decorated_function(*args, **kwargs):
        req = args[0]
        user_id = await check_auth(req)
        if user_id:
            req["user_id"] = user_id.strip()
        return await f(*args, **kwargs)

    return decorated_function
