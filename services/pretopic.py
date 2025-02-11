import concurrent.futures
from typing import Dict, List, Tuple

from txtai.embeddings import Embeddings

from services.logger import root_logger as logger


class TopicClassifier:
    def __init__(self, shouts_by_topic: Dict[str, str], publications: List[Dict[str, str]]):
        """
        Инициализация классификатора тем и поиска публикаций.
        Args:
            shouts_by_topic: Словарь {тема: текст_всех_публикаций}
            publications: Список публикаций с полями 'id', 'title', 'text'
        """
        self.shouts_by_topic = shouts_by_topic
        self.topics = list(shouts_by_topic.keys())
        self.publications = publications
        self.topic_embeddings = None  # Для классификации тем
        self.search_embeddings = None  # Для поиска публикаций
        self._initialization_future = None
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def initialize(self) -> None:
        """
        Асинхронная инициализация векторных представлений.
        """
        if self._initialization_future is None:
            self._initialization_future = self._executor.submit(self._prepare_embeddings)
            logger.info("Векторизация текстов начата в фоновом режиме...")

    def _prepare_embeddings(self) -> None:
        """
        Подготавливает векторные представления для тем и поиска.
        """
        logger.info("Начинается подготовка векторных представлений...")

        # Модель для русского языка
        # TODO: model local caching
        model_path = "sentence-transformers/paraphrase-multilingual-mpnet-base-v2"

        # Инициализируем embeddings для классификации тем
        self.topic_embeddings = Embeddings(path=model_path)
        topic_documents = [(topic, text) for topic, text in self.shouts_by_topic.items()]
        self.topic_embeddings.index(topic_documents)

        # Инициализируем embeddings для поиска публикаций
        self.search_embeddings = Embeddings(path=model_path)
        search_documents = [(str(pub["id"]), f"{pub['title']} {pub['text']}") for pub in self.publications]
        self.search_embeddings.index(search_documents)

        logger.info("Подготовка векторных представлений завершена.")

    def predict_topic(self, text: str) -> Tuple[float, str]:
        """
        Предсказывает тему для заданного текста из известного набора тем.
        Args:
            text: Текст для классификации
        Returns:
            Tuple[float, str]: (уверенность, тема)
        """
        if not self.is_ready():
            logger.error("Векторные представления не готовы. Вызовите initialize() и дождитесь завершения.")
            return 0.0, "unknown"

        try:
            # Ищем наиболее похожую тему
            results = self.topic_embeddings.search(text, 1)
            if not results:
                return 0.0, "unknown"

            score, topic = results[0]
            return float(score), topic

        except Exception as e:
            logger.error(f"Ошибка при определении темы: {str(e)}")
            return 0.0, "unknown"

    def search_similar(self, query: str, limit: int = 5) -> List[Dict[str, any]]:
        """
        Ищет публикации похожие на поисковый запрос.
        Args:
            query: Поисковый запрос
            limit: Максимальное количество результатов
        Returns:
            List[Dict]: Список найденных публикаций с оценкой релевантности
        """
        if not self.is_ready():
            logger.error("Векторные представления не готовы. Вызовите initialize() и дождитесь завершения.")
            return []

        try:
            # Ищем похожие публикации
            results = self.search_embeddings.search(query, limit)

            # Формируем результаты
            found_publications = []
            for score, pub_id in results:
                # Находим публикацию по id
                publication = next((pub for pub in self.publications if str(pub["id"]) == pub_id), None)
                if publication:
                    found_publications.append({**publication, "relevance": float(score)})

            return found_publications

        except Exception as e:
            logger.error(f"Ошибка при поиске публикаций: {str(e)}")
            return []

    def is_ready(self) -> bool:
        """
        Проверяет, готовы ли векторные представления.
        """
        return self.topic_embeddings is not None and self.search_embeddings is not None

    def wait_until_ready(self) -> None:
        """
        Ожидает завершения подготовки векторных представлений.
        """
        if self._initialization_future:
            self._initialization_future.result()

    def __del__(self):
        """
        Очистка ресурсов при удалении объекта.
        """
        if self._executor:
            self._executor.shutdown(wait=False)


# Пример использования:
"""
shouts_by_topic = {
    "Спорт": "... большой текст со всеми спортивными публикациями ...",
    "Технологии": "... большой текст со всеми технологическими публикациями ...",
    "Политика": "... большой текст со всеми политическими публикациями ..."
}

publications = [
    {
        'id': 1,
        'title': 'Новый процессор AMD',
        'text': 'Компания AMD представила новый процессор...'
    },
    {
        'id': 2,
        'title': 'Футбольный матч',
        'text': 'Вчера состоялся решающий матч...'
    }
]

# Создание классификатора
classifier = TopicClassifier(shouts_by_topic, publications)
classifier.initialize()
classifier.wait_until_ready()

# Определение темы текста
text = "Новый процессор показал высокую производительность"
score, topic = classifier.predict_topic(text)
print(f"Тема: {topic} (уверенность: {score:.4f})")

# Поиск похожих публикаций
query = "процессор AMD производительность"
similar_publications = classifier.search_similar(query, limit=3)
for pub in similar_publications:
    print(f"\nНайдена публикация (релевантность: {pub['relevance']:.4f}):")
    print(f"Заголовок: {pub['title']}")
    print(f"Текст: {pub['text'][:100]}...")
"""
