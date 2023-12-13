from functools import wraps
import aiohttp
from aiohttp.web import HTTPUnauthorized
from settings import AUTH_URL


async def check_auth(req):
    token = req.headers.get("Authorization")
    if token:
        print(f"[services.auth] checking auth token: {token}")

        gql = {
            "query": "query { validate_jwt_token( params: ValidateJWTTokenInput!) { is_valid claims } }",
            "variables": {"params": {"token_type": "access_token", "token": token}},
        }

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30.0)) as session:
            async with session.post(AUTH_URL, json=gql) as response:
                response_text = await response.text()
                print(f"[services.auth] response text: {response_text}")

                if response.status != 200:
                    return False, None
                r = await response.json()
                print(f"[services.auth] response: {r}")
                try:
                    data = r.get("data")
                    is_authenticated = False
                    user_id = None
                    if data:
                        result = data.get("validate_jwt_token", {})
                        is_authenticated = result.get("is_valid")
                        if is_authenticated:
                            user_id = result.get("claims", {}).get("sub")
                    return is_authenticated, user_id
                except Exception as e:
                    print(f"{e}: {r}")
    return False, None


def login_required(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        context = info.context
        print(context)
        req = context.get("request")
        is_authenticated, user_id = await check_auth(req)
        if not is_authenticated:
            raise Exception("You are not logged in")
        else:
            # Add user_id to the context
            context["user_id"] = user_id

        # If the user is authenticated, execute the resolver
        return await f(*args, **kwargs)

    return decorated_function


def auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        is_authenticated, user_id = await check_auth(req)
        if not is_authenticated:
            raise HTTPUnauthorized(text="Please, login first")
        else:
            req["user_id"] = user_id
        return await f(*args, **kwargs)

    return decorated_function
