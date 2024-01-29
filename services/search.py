import asyncio
import json
import logging
import os
from typing import List

from elasticsearch import Elasticsearch

from orm.shout import Shout  # Adjust the import as needed
from services.rediscache import redis  # Adjust the import as needed


logger = logging.getLogger('[services.search] ')
logger.setLevel(logging.DEBUG)

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', 'localhost').replace('https://', '').replace('http://', '')
ELASTIC_USER = os.environ.get('ELASTIC_USER', '')
ELASTIC_PASSWORD = os.environ.get('ELASTIC_PASSWORD', '')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT', 9200)
ELASTIC_AUTH = f'{ELASTIC_USER}:{ELASTIC_PASSWORD}' if ELASTIC_USER else ''
ELASTIC_URL = f'https://{ELASTIC_AUTH}@{ELASTIC_HOST}:{ELASTIC_PORT}'


class OpenSearchService:
    def __init__(self, index_name, delete_index_on_startup):
        self.index_name = index_name
        self.delete_index_on_startup = delete_index_on_startup
        self.elasticsearch_client = Elasticsearch(f'{ELASTIC_URL}')

        if self.delete_index_on_startup:
            self.delete_index()

        self.check_index()

    def delete_index(self):
        self.elasticsearch_client.indices.delete(index=self.index_name, ignore_unavailable=True)

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
                    'body': {
                        'type': 'text',
                        'analyzer': 'ru',
                    },
                    'text': {'type': 'text'},
                    'author': {'type': 'text'},
                }
            },
        }

        self.elasticsearch_client.indices.create(index=self.index_name, body=index_settings)
        self.elasticsearch_client.indices.close(index=self.index_name)
        self.elasticsearch_client.indices.open(index=self.index_name)

    def put_mapping(self):
        mapping = {
            'properties': {
                'body': {
                    'type': 'text',
                    'analyzer': 'ru',
                },
                'text': {'type': 'text'},
                'author': {'type': 'text'},
            }
        }

        self.elasticsearch_client.indices.put_mapping(index=self.index_name, body=mapping)

    def check_index(self):
        if not self.elasticsearch_client.indices.exists(index=self.index_name):
            logger.debug(f'Creating {self.index_name} index')
            self.create_index()
            self.put_mapping()

    def index_post(self, shout):
        id_ = str(shout.id)
        logger.debug(f'Indexing post id {id_}')

        self.elasticsearch_client.index(index=self.index_name, id=id_, body=shout)

    def search_post(self, query, limit, offset):
        logger.debug(f'Search query = {query}, limit = {limit}')
        search_body = {
            'query': {
                'match': {
                    '_all': query,
                }
            }
        }

        search_response = self.elasticsearch_client.search(
            index=self.index_name, body=search_body, size=limit, from_=offset
        )
        hits = search_response['hits']['hits']

        return [
            {
                **hit['_source'],
                'score': hit['_score'],
            }
            for hit in hits
        ]


class SearchService:
    lock = asyncio.Lock()
    elastic = None

    @staticmethod
    async def init():
        self = SearchService
        async with self.lock:
            logging.info('Initializing SearchService')
            try:
                self.elastic = OpenSearchService('shouts_index', False)
            except Exception as exc:
                logger.error(exc)

    @staticmethod
    async def search(text: str, limit: int = 50, offset: int = 0) -> List[Shout]:
        payload = []
        self = SearchService
        try:
            # TODO: add ttl for redis cached search results
            cached = await redis.execute('GET', text)
            if not cached:
                async with self.lock:
                    # Use OpenSearchService.search_post method
                    payload = await self.elastic.search_post(text, limit, offset)
                    # Use Redis as cache
                    await redis.execute('SET', text, json.dumps(payload))
            elif isinstance(cached, str):
                payload = json.loads(cached)
        except Exception as e:
            logging.error(f'Error during search: {e}')
        return payload
