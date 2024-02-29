import json
import os
from multiprocessing import Manager
import threading

from opensearchpy import OpenSearch

from services.logger import root_logger as logger
from services.rediscache import redis

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', '').replace('https://', '')
ELASTIC_USER = os.environ.get('ELASTIC_USER', '')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD', '')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', 9200)
ELASTIC_AUTH = f'{ELASTIC_USER}:{ELASTIC_PASSWORD}' if ELASTIC_USER else ''
ELASTIC_URL = os.environ.get(
    'ELASTIC_URL', f'https://{ELASTIC_AUTH}@{ELASTIC_HOST}:{ELASTIC_PORT}'
)
REDIS_TTL = 86400  # 1 day in seconds


index_settings = {
    'settings': {
        'index': {'number_of_shards': 1, 'auto_expand_replicas': '0-all'},
        'analysis': {
            'analyzer': {
                'ru': {
                    'tokenizer': 'standard',
                    'filter': ['lowercase', 'ru_stop', 'ru_stemmer'],
                }
            },
            'filter': {
                'ru_stemmer': {'type': 'stemmer', 'language': 'russian'},
                'ru_stop': {'type': 'stop', 'stopwords': '_russian_'},
            },
        },
    },
    'mappings': {
        'properties': {
            'body': {'type': 'text', 'analyzer': 'ru'},
            'title': {'type': 'text', 'analyzer': 'ru'},
            'subtitle': {'type': 'text', 'analyzer': 'ru'},
            'lead': {'type': 'text', 'analyzer': 'ru'},
            # 'author': {'type': 'text'},
        }
    },
}

expected_mapping = index_settings['mappings']


class SearchService:
    def __init__(self, index_name='search_index'):
        self.index_name = index_name
        self.manager = Manager()
        self.client = None

        # Используем менеджер для создания Lock и Value
        self.lock = threading.Lock()
        self.initialized_flag = self.manager.Value('i', 0)

        # Only initialize the instance if it's not already initialized
        if not self.initialized_flag.value and ELASTIC_HOST:
            try:
                self.client = OpenSearch(
                    hosts=[{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}],
                    http_compress=True,
                    http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
                    use_ssl=True,
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                    # ca_certs = ca_certs_path
                )
                logger.info(' Клиент OpenSearch.org подключен')
                if self.lock.acquire(blocking=False):
                    try:
                        self.check_index()
                    finally:
                        self.lock.release()
                else:
                    logger.debug(' проверка пропущена')
            except Exception as exc:
                logger.error(f' {exc}')
                self.client = None

    def info(self):
        if isinstance(self.client, OpenSearch):
            logger.info(' Поиск подключен')  # : {self.client.info()}')
        else:
            logger.info(' * Задайте переменные среды для подключения к серверу поиска')

    def delete_index(self):
        if self.client:
            logger.debug(f' Удаляем индекс {self.index_name}')
            self.client.indices.delete(index=self.index_name, ignore_unavailable=True)

    def create_index(self):
        if self.client:
            if self.lock.acquire(blocking=False):
                try:
                    logger.debug(f'Создается индекс: {self.index_name}')
                    self.delete_index()
                    self.check_index()
                    logger.debug(f'Индексс {self.index_name} создан')
                except Exception as e:
                    logger.debug(f'Ошибка создания индекса: {str(e)}')
                finally:
                    self.lock.release()
            else:
                logger.error('Не получается создать индекс')

    def put_mapping(self):
        if self.client:
            logger.debug(f' Разметка индекации {self.index_name}')
            self.client.indices.put_mapping(
                index=self.index_name, body=expected_mapping
            )

    def check_index(self):
        if self.client:
            if not self.client.indices.exists(index=self.index_name):
                self.create_index()
                self.put_mapping()
            else:
                # Check if the mapping is correct, and recreate the index if needed
                mapping = self.client.indices.get_mapping(index=self.index_name)
                if mapping != expected_mapping:
                    self.recreate_index()

    def recreate_index(self):
        thread = threading.Thread(target=self._recreate_index)
        thread.start()

    def _recreate_index(self):
        if self.lock.acquire(blocking=False):
            try:
                self.delete_index()
                self.check_index()
            finally:
                self.lock.release()
        else:
            logger.debug(' не удалось проиндексировать')

    def index(self, shout):
        if self.client:
            id_ = str(shout.id)
            logger.debug(f' Индексируем пост {id_}')
            self.client.index(index=self.index_name, id=id_, body=shout.dict())

    async def search(self, text, limit, offset):
        logger.debug(f' Ищем: {text}')
        search_body = {'query': {'match': {'_all': text}}}
        if self.client:
            search_response = self.client.search(
                index=self.index_name, body=search_body, size=limit, from_=offset
            )
            hits = search_response['hits']['hits']

            results = [{**hit['_source'], 'score': hit['_score']} for hit in hits]

            # Use Redis as cache with TTL
            redis_key = f'search:{text}'
            await redis.execute('SETEX', redis_key, REDIS_TTL, json.dumps(results))
        return []


search_service = SearchService()


async def search_text(text: str, limit: int = 50, offset: int = 0):
    payload = []
    if search_service.client:
        # Use OpenSearchService.search_post method
        payload = await search_service.search(text, limit, offset)
    return payload
