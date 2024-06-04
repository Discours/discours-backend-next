import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger("exception")
logging.basicConfig(level=logging.DEBUG)


class ExceptionHandlerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            response = await call_next(request)
            return response
        except Exception as exc:
            logger.exception(exc)
            return JSONResponse({"detail": "An error occurred. Please try again later."}, status_code=500)
