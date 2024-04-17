import os
import re

from starlette.endpoints import HTTPEndpoint
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from orm.author import Author
from services.db import local_session


class WebhookEndpoint(HTTPEndpoint):
    async def post(self, request: Request) -> JSONResponse:
        try:
            data = await request.json()
            if not data:
                raise HTTPException(status_code=400, detail="Request body is empty")
            auth = request.headers.get("Authorization")
            if not auth or auth != os.environ.get("WEBHOOK_SECRET"):
                raise HTTPException(
                    status_code=401, detail="Invalid Authorization header"
                )
            # logger.debug(data)
            user = data.get("user")
            if not isinstance(user, dict):
                raise HTTPException(
                    status_code=400, detail="User data is not a dictionary"
                )
            #
            name: str = (
                f"{user.get('given_name', user.get('slug'))} {user.get('middle_name', '')}"
                + f"{user.get('family_name', '')}".strip()
            ) or "Аноним"
            user_id: str = user.get("id", "")
            email: str = user.get("email", "")
            pic: str = user.get("picture", "")
            if user_id:

                with local_session() as session:
                    author = session.query(Author).filter(Author.user == user_id).first()
                    if not author:
                        # If the author does not exist, create a new one
                        slug: str = email.split("@")[0].replace(".", "-").lower()
                        slug: str = re.sub("[^0-9a-z]+", "-", slug)
                        while True:
                            author = (
                                session.query(Author).filter(Author.slug == slug).first()
                            )
                            if not author:
                                break
                            slug = f"{slug}-{len(session.query(Author).filter(Author.email == email).all()) + 1}"
                        author = Author(user=user_id, slug=slug, name=name, pic=pic)
                        session.add(author)
                        session.commit()

            return JSONResponse({"status": "success"})
        except HTTPException as e:
            return JSONResponse(
                {"status": "error", "message": str(e.detail)}, status_code=e.status_code
            )
        except Exception as e:
            import traceback

            traceback.print_exc()
            return JSONResponse({"status": "error", "message": str(e)}, status_code=500)
