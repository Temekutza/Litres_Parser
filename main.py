"""
Entry point.

Examples:
  python main.py discover --limit 1000
  python main.py crawl --discover --discover-limit 5000 --workers 5 --limit 1000
  python main.py export --out litres.xlsx
  python main.py single https://www.litres.ru/book/...
"""

import sys


def _ensure_supported_python() -> None:
    # The project uses modern typing syntax (e.g. `list[str]`, `int | None`),
    # so we require Python 3.10+.
    if sys.version_info < (3, 10):
        v = ".".join(map(str, sys.version_info[:3]))
        raise SystemExit(
            "This project requires Python 3.10+.\n"
            f"Current Python: {v}\n"
            "Install a newer Python and recreate the virtualenv.\n"
            "Windows: https://www.python.org/downloads/windows/"
        )


if __name__ == "__main__":
    _ensure_supported_python()
    from litres_parser.cli import main

    main()
