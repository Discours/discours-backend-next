from functools import wraps
from httpx import AsyncClient, HTTPError
from settings import AUTH_URL


async def check_auth(req):
    token = req.headers.get("Authorization")
    print(f"[services.auth] checking auth token: {token}")

    query_name = "session"
    query_type = "query"
    operation = "GetUserId"

    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    gql = {
        "query": query_type + " " + operation + " { " + query_name + " { user { id } } " + " }",
        "operationName": operation,
        "variables": None,
    }

    async with AsyncClient(timeout=30.0) as client:
        response = await client.post(AUTH_URL, headers=headers, json=gql)
        print(f"[services.auth] response: {response.status_code} {response.text}")
        if response.status_code != 200:
            return False, None
        r = response.json()
        try:
            user_id = (
                r.get("data", {}).get(query_name, {}).get("user", {}).get("id", None)
            )
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
            # Добавляем author_id в контекст
            context["author_id"] = user_id

        # Если пользователь аутентифицирован, выполняем резолвер
        return await f(*args, **kwargs)

    return decorated_function


def auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        is_authenticated, user_id = await check_auth(req)
        if not is_authenticated:
            raise HTTPError("please, login first")
        else:
            req["author_id"] = user_id
        return await f(*args, **kwargs)

    return decorated_function
