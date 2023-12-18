from functools import wraps

import aiohttp
from aiohttp.web import HTTPUnauthorized

from settings import AUTH_URL


async def check_auth(req) -> str | None:
    token = req.headers.get("Authorization")
    user_id = ""
    if token:
        # Logging the authentication token
        print(f"[services.auth] checking auth token: {token}")
        query_name = "validate_jwt_token"
        operation = "ValidateToken"
        headers = {
            "Content-Type": "application/json",
        }

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
        try:
            # Asynchronous HTTP request to the authentication server
            async with aiohttp.ClientSession() as session:
                async with session.post(AUTH_URL, json=gql, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        errors = data.get("errors")
                        if errors:
                            print(f"[services.auth] errors: {errors}")
                        else:
                            user_id = data.get("data", {}).get(query_name, {}).get("claims", {}).get("sub")
                            return user_id
        except Exception as e:
            # Handling and logging exceptions during authentication check
            print(f"[services.auth] {e}")

    if not user_id:
        raise HTTPUnauthorized(text="Please, login first")


def login_required(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        context = info.context
        req = context.get("request")
        user_id = await check_auth(req)
        if user_id:
            context["user_id"] = user_id
        return await f(*args, **kwargs)

    return decorated_function


def auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        user_id = await check_auth(req)
        if user_id:
            req["user_id"] = user_id
        return await f(*args, **kwargs)

    return decorated_function
