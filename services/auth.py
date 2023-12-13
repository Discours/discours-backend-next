from functools import wraps
import aiohttp
from aiohttp.web import HTTPUnauthorized

from resolvers import get_author_id
from settings import AUTH_URL


async def check_auth(req) -> (int | None, bool):
    token = req.headers.get("Authorization")
    if token:
        # Logging the authentication token
        print(f"[services.auth] checking auth token: {token}")
        query_name = "validate_jwt_token"
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
            "query": f"query ValidateToken($params: ValidateJWTTokenInput!) {{ {query_name}(params: $params) {{ is_valid claims }} }}",
            "variables": variables,
        }

        try:
            # Asynchronous HTTP request to the authentication server
            async with aiohttp.ClientSession() as session:
                async with session.post(AUTH_URL, json=gql, headers=headers) as response:
                    # Logging the GraphQL response
                    response_text = await response.text()
                    print(f"GraphQL Response: {response_text}")

                    if response.status == 200:
                        # Parsing JSON response
                        data = await response.json()
                        user_id = data.get("data", {}).get(query_name, {}).get("claims", {}).get("sub")

                        if user_id:
                            # Logging the retrieved user ID
                            print(f"User ID retrieved: {user_id}")
                            # Fetching author information
                            author = get_author_id(None, None, user_id)
                            return author.id, True
                        else:
                            # Logging when no user ID is found in the response
                            print("No user ID found in the response")
                    else:
                        # Logging when the request to the authentication server fails
                        print(f"Request failed with status: {response.status}")

        except Exception as e:
            # Handling and logging exceptions during authentication check
            print(f"Exception during authentication check: {e}")

    return False, None


def login_required(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        info = args[1]
        context = info.context
        print(context)
        req = context.get("request")
        # Performing authentication check
        is_authenticated, author_id = await check_auth(req)
        if not is_authenticated:
            # Raising an exception if the user is not authenticated
            raise HTTPUnauthorized(text="Please, login first")
        else:
            # Adding user_id to the context
            context["author_id"] = author_id

        # If the user is authenticated, execute the resolver
        return await f(*args, **kwargs)

    return decorated_function


def auth_request(f):
    @wraps(f)
    async def decorated_function(*args, **kwargs):
        req = args[0]
        # Performing authentication check
        is_authenticated, author_id = await check_auth(req)
        if not is_authenticated:
            # Raising HTTPUnauthorized exception if the user is not authenticated
            raise HTTPUnauthorized(text="Please, login first")
        else:
            # Modifying the req with the author_id
            req["author_id"] = author_id
        return await f(*args, **kwargs)

    return decorated_function
