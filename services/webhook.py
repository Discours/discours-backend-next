import os
import re

from starlette.endpoints import HTTPEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse

from orm.author import Author
from resolvers.author import create_author
from services.db import local_session


class WebhookEndpoint(HTTPEndpoint):
    async def post(self, request: Request) -> JSONResponse:
        try:
            data = await request.json()
            if data:
                auth = request.headers.get("Authorization")
                if auth:
                    if auth == os.environ.get("WEBHOOK_SECRET"):
                        user_id: str = data["user"]["id"]
                        name: str = data["user"]["given_name"]
                        slug: str = data["user"]["email"].split("@")[0]
                        slug: str = re.sub("[^0-9a-z]+", "-", slug.lower())
                        with local_session() as session:
                            author = (
                                session.query(Author)
                                .filter(Author.slug == slug)
                                .first()
                            )
                            if author:
                                slug = slug + "-" + user_id.split("-").pop()
                            await create_author(user_id, slug, name)

            return JSONResponse({"status": "success"})
        except Exception as e:
            import traceback

            traceback.print_exc()
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
