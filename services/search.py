import json
import logging
import os
from multiprocessing import Manager

from opensearchpy import OpenSearch

from services.rediscache import redis


logger = logging.getLogger('\t[services.search]\t')
logger.setLevel(logging.DEBUG)

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', '').replace('https://', '').replace('http://', '')
ELASTIC_USER = os.environ.get('ELASTIC_USER', '')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD', '')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', 9200)
ELASTIC_AUTH = f'{ELASTIC_USER}:{ELASTIC_PASSWORD}' if ELASTIC_USER else ''
ELASTIC_URL = os.environ.get('ELASTIC_URL', f'https://{ELASTIC_AUTH}@{ELASTIC_HOST}:{ELASTIC_PORT}')
REDIS_TTL = 86400  # 1 day in seconds


class SearchService:
    def __init__(self, index_name='search_index'):
        self.index_name = index_name
        self.disabled = False
        self.manager = Manager()
        self.client = None

        # Используем менеджер для создания Lock и Value
        self.lock = self.manager.Lock()
        self.initialized_flag = self.manager.Value('i', 0)

        # Only initialize the instance if it's not already initialized
        if not self.initialized_flag.value and ELASTIC_HOST:
            logger.info(' инициализация клиента OpenSearch.org')
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

            except Exception as exc:
                logger.error(exc)
                self.disabled = True

            self.check_index()
        else:
            self.disabled = True

    def info(self):
        if self.client:
            logger.info(f'{self.client}')
        else:
            logger.info(' * Задайте переменные среды для подключения к серверу поиска')

    def delete_index(self):
        if not self.disabled and self.client:
            self.client.indices.delete(index=self.index_name, params={'ignore_unavailable': True})

    def create_index(self):
        index_settings = {
            'settings': {
                'index': {
                    'number_of_shards': 1,
                    'auto_expand_replicas': '0-all',
                },
                'analysis': {
                    'analyzer': {
                        'ru': {
                            'tokenizer': 'standard',
                            'filter': ['lowercase', 'ru_stop', 'ru_stemmer'],
                        }
                    },
                    'filter': {
                        'ru_stemmer': {
                            'type': 'stemmer',
                            'language': 'russian',
                        },
                        'ru_stop': {
                            'type': 'stop',
                            'stopwords': '_russian_',
                        },
                    },
                },
            },
            'mappings': {
                'properties': {
                    'body': {'type': 'text', 'analyzer': 'ru'},
                    'text': {'type': 'text'},
                    'author': {'type': 'text'},
                }
            },
        }
        try:
            if self.client:
                with self.lock:
                    self.client.indices.create(index=self.index_name, body=index_settings)
                    self.client.indices.close(index=self.index_name)
                    self.client.indices.open(index=self.index_name)
        except Exception as error:
            logger.warn(error)
            self.disabled = True

    def put_mapping(self):
        mapping = {
            'properties': {
                'body': {'type': 'text', 'analyzer': 'ru'},
                'text': {'type': 'text'},
                'author': {'type': 'text'},
            }
        }
        if self.client:
            self.client.indices.put_mapping(index=self.index_name, body=mapping)

    def check_index(self):
        if self.client:
            if not self.client.indices.exists(index=self.index_name) and not self.disabled:
                logger.debug(f'Creating {self.index_name} index')
                self.create_index()
                self.put_mapping()
            else:
                # Check if the mapping is correct, and recreate the index if needed
                mapping = self.client.indices.get_mapping(index=self.index_name)
                expected_mapping = {
                    'properties': {
                        'body': {'type': 'text', 'analyzer': 'ru'},
                        'text': {'type': 'text'},
                        'author': {'type': 'text'},
                    }
                }
                if mapping != expected_mapping:
                    logger.debug(f'Recreating {self.index_name} index due to incorrect mapping')
                    self.recreate_index()

    def recreate_index(self):
        with self.lock:
            self.delete_index()
            self.check_index()

    def index_post(self, shout):
        if not not self.disabled and self.client:
            id_ = str(shout.id)
            logger.debug(f'Indexing post id {id_}')
            self.client.index(index=self.index_name, id=id_, body=shout)

    def search_post(self, query, limit, offset):
        logger.debug(f'query: {query}')
        search_body = {
            'query': {'match': {'_all': query}},
        }
        if self.client:
            search_response = self.client.search(index=self.index_name, body=search_body, size=limit, from_=offset)
            hits = search_response['hits']['hits']

            return [
                {
                    **hit['_source'],
                    'score': hit['_score'],
                }
                for hit in hits
            ]
        return []


search = SearchService()


async def search_text(text: str, limit: int = 50, offset: int = 0):
    payload = []
    try:
        # Use a key with a prefix to differentiate search results from other Redis data
        redis_key = f'search:{text}'
        if not search.disabled:
            # Use OpenSearchService.search_post method
            payload = search.search_post(text, limit, offset)
            # Use Redis as cache with TTL
            await redis.execute('SETEX', redis_key, REDIS_TTL, json.dumps(payload))
    except Exception as e:
        logging.error(f'Error during search: {e}')
    return payload
