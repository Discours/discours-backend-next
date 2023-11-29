from functools import wraps
import aiohttp
from aiohttp.web import HTTPUnauthorized
from settings import AUTH_URL


async def check_auth(req):
    token = req.headers.get("Authorization")
    print(f"[services.auth] checking auth token: {token}")

    query_name = "session"
    query_type = "query"
    operation = "GetUserId"

    headers = {"Authorization": token, "Content-Type": "application/json"}

    gql = {
        "query": query_type + " " + operation + " { " + query_name + " { user { id } } " + " }",
        "operationName": operation,
        "variables": None,
    }

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30.0)) as session:
        async with session.post(AUTH_URL, headers=headers, json=gql) as response:
            print(f"[services.auth] response: {response.status} {await response.text()}")
            if response.status != 200:
                return False, None
            r = await response.json()
            try:
                user_id = r.get("data", {}).get(query_name, {}).get("user", {}).get("id", None)
                is_authenticated = user_id is not None
                return is_authenticated, user_id
            except Exception as e:
                print(f"{e}: {r}")
                return False, None


def login_required(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        context = info.context
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
