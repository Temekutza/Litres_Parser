## LitRes Parser

Парсер **метаданных** книг с `litres.ru` (URL, название, автор(ы), рейтинг, цена, жанры, описание) с сохранением прогресса в SQLite и экспортом в XLSX.

Важно: каталог LitRes очень большой, поэтому «все книги» — это **долгий** процесс. Используйте лимиты, паузы и продолжение из базы.

### Требования

- Python **3.10+**

### Установка

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Быстрый старт

- **1) Найти ссылки на книги и положить в очередь (по умолчанию через каталог жанров):**

```bash
python main.py discover --limit 5000
```

- **2) Спарсить очередь:**

```bash
python main.py crawl --workers 5 --limit 1000
```

- **3) Экспорт в Excel:**

```bash
python main.py export --out litres.xlsx
```

> Если нужны отзывы: добавьте `--with-reviews` к `crawl` (медленнее, best-effort).

### Режим «одна книга»

```bash
python main.py single https://www.litres.ru/book/...
```

### Как это работает

- Discovery по умолчанию обходит каталог жанров (`/pages/new_genres/`) и постранично собирает URL’ы.
- Альтернатива: `--method sitemap` (если сайт публикует `Sitemap:` в `robots.txt`; сейчас это может отсутствовать).
- Crawl берёт URL’ы из очереди в SQLite и вытаскивает метаданные со страницы (в первую очередь из JSON‑LD и meta-тегов).
- Прогресс сохраняется в `litres.sqlite` — можно запускать повторно.

### Официальный API (рекомендовано для легального массового доступа)

Если вам нужен гарантированный каталог/выгрузки «по правилам», лучше использовать партнёрский API LitRes: [docs.litres.ru](https://docs.litres.ru/public/1247015.html).


