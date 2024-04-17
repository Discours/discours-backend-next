import asyncio
import json
import os

from opensearchpy import OpenSearch

from services.encoders import CustomJSONEncoder
from services.logger import root_logger as logger
from services.rediscache import redis

ELASTIC_HOST = os.environ.get("ELASTIC_HOST", "").replace("https://", "")
ELASTIC_USER = os.environ.get("ELASTIC_USER", "")
ELASTIC_PASSWORD = os.environ.get("ELASTIC_PASSWORD", "")
ELASTIC_PORT = os.environ.get("ELASTIC_PORT", 9200)
ELASTIC_AUTH = f"{ELASTIC_USER}:{ELASTIC_PASSWORD}" if ELASTIC_USER else ""
ELASTIC_URL = os.environ.get(
    "ELASTIC_URL", f"https://{ELASTIC_AUTH}@{ELASTIC_HOST}:{ELASTIC_PORT}"
)
REDIS_TTL = 86400  # 1 day in seconds

index_settings = {
    "settings": {
        "index": {"number_of_shards": 1, "auto_expand_replicas": "0-all"},
        "analysis": {
            "analyzer": {
                "ru": {
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ru_stop", "ru_stemmer"],
                }
            },
            "filter": {
                "ru_stemmer": {"type": "stemmer", "language": "russian"},
                "ru_stop": {"type": "stop", "stopwords": "_russian_"},
            },
        },
    },
    "mappings": {
        "properties": {
            "body": {"type": "text", "analyzer": "ru"},
            "title": {"type": "text", "analyzer": "ru"},
            "subtitle": {"type": "text", "analyzer": "ru"},
            "lead": {"type": "text", "analyzer": "ru"},
            # 'author': {'type': 'text'},
        }
    },
}

expected_mapping = index_settings["mappings"]

# Create an event loop
search_loop = asyncio.get_event_loop()


class SearchService:
    def __init__(self, index_name="search_index"):
        self.index_name = index_name
        self.client = None
        self.lock = asyncio.Lock()  # Create an asyncio lock

        # Only initialize the instance if it's not already initialized
        if ELASTIC_HOST:
            try:
                self.client = OpenSearch(
                    hosts=[{"host": ELASTIC_HOST, "port": ELASTIC_PORT}],
                    http_compress=True,
                    http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
                    use_ssl=True,
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                    # ca_certs = ca_certs_path
                )
                logger.info(" Клиент OpenSearch.org подключен")

                # Create a task and run it in the event loop
                search_loop.create_task(self.check_index())
            except Exception as exc:
                logger.error(f" {exc}")
                self.client = None

    def info(self):
        if isinstance(self.client, OpenSearch):
            logger.info(" Поиск подключен")  # : {self.client.info()}')
        else:
            logger.info(" * Задайте переменные среды для подключения к серверу поиска")

    def delete_index(self):
        if self.client:
            logger.debug(f" Удаляем индекс {self.index_name}")
            self.client.indices.delete(index=self.index_name, ignore_unavailable=True)

    def create_index(self):
        if self.client:
            logger.debug(f"Создается индекс: {self.index_name}")
            self.delete_index()
            self.client.indices.create(index=self.index_name, body=index_settings)
            logger.debug(f"Индекс {self.index_name} создан")

    async def check_index(self):
        if self.client:
            logger.debug(f" Проверяем индекс {self.index_name}...")
            if not self.client.indices.exists(index=self.index_name):
                self.create_index()
                self.client.indices.put_mapping(
                    index=self.index_name, body=expected_mapping
                )
            else:
                logger.info(f"найден существующий индекс {self.index_name}")
                # Check if the mapping is correct, and recreate the index if needed
                result = self.client.indices.get_mapping(index=self.index_name)
                if isinstance(result, str):
                    result = json.loads(result)
                if isinstance(result, dict):
                    mapping = result.get("mapping")
                    if mapping and mapping != expected_mapping:
                        logger.debug(f" найдена структура индексации: {mapping}")
                        logger.warn(
                            " требуется другая структура индексации, переиндексация"
                        )
                        await self.recreate_index()

    async def recreate_index(self):
        if self.client:
            async with self.lock:
                self.client.indices.delete(
                    index=self.index_name, ignore_unavailable=True
                )
                await self.check_index()

    def index(self, shout):
        if self.client:
            id_ = str(shout.id)
            logger.debug(f" Индексируем пост {id_}")
            asyncio.create_task(self.perform_index(shout))

    async def perform_index(self, shout):
        if self.client:
            self.client.index(
                index=self.index_name, id=str(shout.id), body=shout.dict()
            )

    async def search(self, text, limit, offset):
        logger.debug(f" Ищем: {text}")
        search_body = {"query": {"match": {"_all": text}}}
        if self.client:
            search_response = self.client.search(
                index=self.index_name, body=search_body, size=limit, from_=offset
            )
            hits = search_response["hits"]["hits"]

            results = [{**hit["_source"], "score": hit["_score"]} for hit in hits]

            # Use Redis as cache with TTL
            redis_key = f"search:{text}"
            await redis.execute(
                "SETEX",
                redis_key,
                REDIS_TTL,
                json.dumps(results, cls=CustomJSONEncoder),
            )
        return []


search_service = SearchService()


async def search_text(text: str, limit: int = 50, offset: int = 0):
    payload = []
    if search_service.client:
        # Use OpenSearchService.search_post method
        payload = await search_service.search(text, limit, offset)
    return payload
