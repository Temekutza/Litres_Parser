"""
Нормализация данных перед сохранением в базу.
Все данные нормализуются на этапе парсинга, а не экспорта.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def normalize_date(iso_date: str | None) -> str:
    """
    Преобразует ISO дату в DD.MM.YYYY
    
    Примеры:
        2023-11-20T13:48:18 → 20.11.2023
        2023-11-20T13:48:18+00:00 → 20.11.2023
        2023-11-20 → 20.11.2023
    """
    if not iso_date or not isinstance(iso_date, str):
        return ""
    try:
        # Обрабатываем разные форматы ISO
        dt = datetime.fromisoformat(iso_date.replace('Z', '+00:00'))
        return dt.strftime("%d.%m.%Y")
    except (ValueError, AttributeError):
        return iso_date


def normalize_price(price: str | None) -> str:
    """
    Преобразует цену в числовой формат
    
    Примеры:
        569 RUB → 569,00
        1234.50 RUB → 1234,50
        Free → 0,00
    """
    if not price or not isinstance(price, str):
        return ""
    
    # Извлекаем только число
    match = re.search(r'(\d+(?:[.,]\d+)?)', price)
    if match:
        num = match.group(1).replace(',', '.')
        try:
            return f"{float(num):.2f}".replace('.', ',')
        except ValueError:
            return ""
    
    return ""


def normalize_age_restriction(age: str | None) -> str:
    """
    Преобразует возрастное ограничение в число
    
    Примеры:
        18+ → 18
        16+ → 16
        0+ → 0
    """
    if not age or not isinstance(age, str):
        return ""
    
    # Извлекаем только цифры
    match = re.search(r'(\d+)', age)
    return match.group(1) if match else ""


def normalize_series_title(series: str | None) -> str:
    """
    Извлекает названия серий без префиксов
    
    Примеры:
        Входит в серию «Иронический детектив (Эксмо)»
        1 книга из 2 в серии «Кошмары Чернолучья»
        →
        Иронический детектив (Эксмо)
        
        Кошмары Чернолучья
    """
    if not series or not isinstance(series, str):
        return ""
    
    # Находим все серии в кавычках
    series_names = re.findall(r'«([^»]+)»', series)
    if series_names:
        return "\n\n".join(series_names)
    
    # Если кавычек нет, убираем типовые префиксы
    result = re.sub(r'^Входит в серию\s+', '', series, flags=re.IGNORECASE)
    result = re.sub(r'\d+\s+книга\s+из\s+\d+\s+в\s+серии\s+', '', result, flags=re.IGNORECASE)
    return result.strip()


def normalize_avatar_url(avatar: str | None) -> str:
    """
    Преобразует URL аватарки в 'есть' или пустую строку
    
    Примеры:
        /pub/avatar/100/00/04/52/31/40/40/452314040.jpg → есть
        https://... → есть
        None → (пусто)
    """
    if not avatar or not isinstance(avatar, str):
        return ""
    
    # Если есть URL (начинается с / или http), возвращаем "есть"
    if avatar.startswith('/') or avatar.startswith('http'):
        return "есть"
    
    return ""


def normalize_rating(rating: str | None) -> str:
    """
    Нормализует рейтинг (оставляет как есть, но проверяет формат)
    
    Примеры:
        4.9 → 4.9
        4,9 → 4.9
        5 → 5.0
    """
    if not rating or not isinstance(rating, str):
        return ""
    
    # Заменяем запятую на точку
    rating = rating.replace(',', '.')
    
    # Проверяем, что это число
    try:
        float_rating = float(rating)
        return str(float_rating)
    except ValueError:
        return ""


def normalize_count(count: str | None) -> str:
    """
    Нормализует счетчики (извлекает только число)
    
    Примеры:
        547 оценок → 547
        1 234 → 1234
        25+ → 25
    """
    if not count or not isinstance(count, str):
        return ""
    
    # Убираем пробелы и извлекаем число
    count = count.replace(' ', '').replace('\xa0', '')
    match = re.search(r'(\d+)', count)
    return match.group(1) if match else ""


def normalize_boolean(value: str | int | bool | None) -> str:
    """
    Нормализует булевы значения в '1' или '0'
    
    Примеры:
        True → 1
        1 → 1
        'yes' → 1
        False → 0
    """
    if value in (True, 1, '1', 'yes', 'true', 'True'):
        return "1"
    return "0"


def normalize_book_data(raw_book: dict[str, Any]) -> dict[str, Any]:
    """
    Нормализует все поля книги
    
    Args:
        raw_book: Сырые данные книги из парсера
        
    Returns:
        Нормализованные данные готовые для сохранения в БД
    """
    normalized = {}
    
    # URL и title остаются как есть
    normalized["url"] = raw_book.get("url", "")
    normalized["title"] = raw_book.get("title", "")
    normalized["authors"] = raw_book.get("authors", "")
    normalized["description"] = raw_book.get("description", "")
    normalized["cover_url"] = raw_book.get("cover_url", "")
    normalized["genres"] = raw_book.get("genres", "")
    
    # Нормализация числовых и форматированных полей
    normalized["price"] = normalize_price(raw_book.get("price"))
    normalized["rating"] = normalize_rating(raw_book.get("rating"))
    normalized["rating_count"] = normalize_count(raw_book.get("rating_count"))
    normalized["livelib_rating"] = normalize_rating(raw_book.get("livelib_rating"))
    normalized["livelib_rating_count"] = normalize_count(raw_book.get("livelib_rating_count"))
    normalized["reviews_count"] = normalize_count(raw_book.get("reviews_count"))
    normalized["quotations_count"] = normalize_count(raw_book.get("quotations_count"))
    normalized["pages"] = normalize_count(raw_book.get("pages"))
    normalized["age_restriction"] = normalize_age_restriction(raw_book.get("age_restriction"))
    
    # Нормализация серий
    normalized["in_series"] = normalize_boolean(raw_book.get("in_series"))
    normalized["series_title"] = normalize_series_title(raw_book.get("series_title"))
    
    # Нормализация форматов
    normalized["format_text"] = normalize_boolean(raw_book.get("format_text"))
    normalized["format_audio"] = normalize_boolean(raw_book.get("format_audio"))
    normalized["format_paper"] = normalize_boolean(raw_book.get("format_paper"))
    
    # Форматы и главы (текстовые, оставляем как есть)
    normalized["formats"] = raw_book.get("formats", "")
    normalized["chapters"] = raw_book.get("chapters", "")
    
    # Метаданные
    normalized["scraped_at"] = raw_book.get("scraped_at", "")
    normalized["status"] = raw_book.get("status", "ok")
    normalized["error"] = raw_book.get("error", "")
    
    return normalized


def normalize_review_data(raw_review: dict[str, Any]) -> dict[str, Any]:
    """
    Нормализует все поля отзыва
    
    Args:
        raw_review: Сырые данные отзыва из парсера
        
    Returns:
        Нормализованные данные готовые для сохранения в БД
    """
    normalized = {}
    
    # Базовые поля
    normalized["review_id"] = raw_review.get("review_id", "")
    normalized["book_url"] = raw_review.get("book_url", "")
    normalized["author"] = raw_review.get("author", "")
    normalized["text"] = raw_review.get("text", "")
    normalized["rating"] = normalize_rating(raw_review.get("rating"))
    
    # Нормализация аватарки и даты
    normalized["author_avatar"] = normalize_avatar_url(raw_review.get("author_avatar"))
    normalized["published_at"] = normalize_date(raw_review.get("published_at"))
    
    # Нормализация счетчиков
    normalized["likes"] = normalize_count(raw_review.get("likes"))
    normalized["dislikes"] = normalize_count(raw_review.get("dislikes"))
    normalized["comments_count"] = normalize_count(raw_review.get("comments_count"))
    normalized["replies_count"] = normalize_count(raw_review.get("replies_count"))
    
    # Нормализация реплаев
    raw_replies = raw_review.get("replies", [])
    normalized["replies"] = [normalize_reply_data(r) for r in raw_replies]
    
    # Булевы поля
    normalized["is_livelib"] = normalize_boolean(raw_review.get("is_livelib"))
    
    # Метаданные (scraped_at добавится в storage.py)
    
    return normalized


def normalize_reply_data(raw_reply: dict[str, Any]) -> dict[str, Any]:
    """
    Нормализует данные реплая
    
    Args:
        raw_reply: Сырые данные реплая
        
    Returns:
        Нормализованные данные
    """
    normalized = {}
    
    normalized["author"] = raw_reply.get("author", "")
    normalized["author_avatar"] = normalize_avatar_url(raw_reply.get("author_avatar"))
    normalized["published_at"] = normalize_date(raw_reply.get("published_at"))
    normalized["text"] = raw_reply.get("text", "")
    normalized["likes"] = normalize_count(raw_reply.get("likes"))
    normalized["dislikes"] = normalize_count(raw_reply.get("dislikes"))
    
    return normalized


def save_raw_json(data: dict[str, Any], filepath: str) -> None:
    """
    Сохраняет сырые данные в JSON файл (для отладки)
    
    Args:
        data: Словарь с данными
        filepath: Путь к файлу
    """
    import json
    from pathlib import Path
    
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def save_normalized_json(data: dict[str, Any], filepath: str) -> None:
    """
    Сохраняет нормализованные данные в JSON файл (для отладки)
    
    Args:
        data: Словарь с данными
        filepath: Путь к файлу
    """
    save_raw_json(data, filepath)
