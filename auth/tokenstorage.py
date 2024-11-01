from datetime import datetime, timedelta, timezone

from base.redis import redis
from validations.auth import AuthInput

from auth.jwtcodec import JWTCodec
from settings import ONETIME_TOKEN_LIFE_SPAN, SESSION_TOKEN_LIFE_SPAN


async def save(token_key, life_span, auto_delete=True):
    await redis.execute("SET", token_key, "True")
    if auto_delete:
        expire_at = (datetime.now(tz=timezone.utc) + timedelta(seconds=life_span)).timestamp()
        await redis.execute("EXPIREAT", token_key, int(expire_at))


class SessionToken:
    @classmethod
    async def verify(cls, token: str):
        """
        Rules for a token to be valid.
            - token format is legal
            - token exists in redis database
            - token is not expired
        """
        try:
            return JWTCodec.decode(token)
        except Exception as e:
            raise e

    @classmethod
    async def get(cls, payload, token):
        return await TokenStorage.get(f"{payload.user_id}-{payload.username}-{token}")


class TokenStorage:
    @staticmethod
    async def get(token_key):
        print("[tokenstorage.get] " + token_key)
        # 2041-user@domain.zn-eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoyMDQxLCJ1c2VybmFtZSI6ImFudG9uLnJld2luK3Rlc3QtbG9hZGNoYXRAZ21haWwuY29tIiwiZXhwIjoxNjcxNzgwNjE2LCJpYXQiOjE2NjkxODg2MTYsImlzcyI6ImRpc2NvdXJzIn0.Nml4oV6iMjMmc6xwM7lTKEZJKBXvJFEIZ-Up1C1rITQ
        return await redis.execute("GET", token_key)

    @staticmethod
    async def create_onetime(user: AuthInput) -> str:
        life_span = ONETIME_TOKEN_LIFE_SPAN
        exp = datetime.now(tz=timezone.utc) + timedelta(seconds=life_span)
        one_time_token = JWTCodec.encode(user, exp)
        await save(f"{user.id}-{user.username}-{one_time_token}", life_span)
        return one_time_token

    @staticmethod
    async def create_session(user: AuthInput) -> str:
        life_span = SESSION_TOKEN_LIFE_SPAN
        exp = datetime.now(tz=timezone.utc) + timedelta(seconds=life_span)
        session_token = JWTCodec.encode(user, exp)
        await save(f"{user.id}-{user.username}-{session_token}", life_span)
        return session_token

    @staticmethod
    async def revoke(token: str) -> bool:
        payload = None
        try:
            print("[auth.tokenstorage] revoke token")
            payload = JWTCodec.decode(token)
        except:  # noqa
            pass
        else:
            await redis.execute("DEL", f"{payload.user_id}-{payload.username}-{token}")
        return True

    @staticmethod
    async def revoke_all(user: AuthInput):
        tokens = await redis.execute("KEYS", f"{user.id}-*")
        await redis.execute("DEL", *tokens)
