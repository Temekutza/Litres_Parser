"""
Entry point.

Examples:
  python main.py discover --limit 1000
  python main.py crawl --discover --discover-limit 5000 --workers 5 --limit 1000
  python main.py export --out litres.xlsx
  python main.py single https://www.litres.ru/book/...
"""

from litres_parser.cli import main


if __name__ == "__main__":
    main()
