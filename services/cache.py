from functools import wraps

from dogpile.cache import make_region

# Создание региона кэша с TTL 300 секунд
cache_region = make_region().configure('dogpile.cache.memory', expiration_time=300)


# Декоратор для кэширования методов
def cache_method(cache_key: str):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Генерация ключа для кэширования
            key = cache_key.format(*args, **kwargs)
            # Получение значения из кэша
            result = cache_region.get(key)
            if result is None:
                # Если значение отсутствует в кэше, вызываем функцию и кэшируем результат
                result = f(*args, **kwargs)
                cache_region.set(key, result)
            return result

        return decorated_function

    return decorator
