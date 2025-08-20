#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse

from main_app.rospakovka import rospakovka


def parser_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--max-pages", type=int, default=50)
    parser.add_argument("--chunk-size", type=int, default=1, help="Chunk size in megabytes")
    return parser.parse_args()


def main(year: int = 2025) -> None:
    print(f"Подключаемся и скачиваем ZIP за {year} год...")
    args = parser_args()

    rospakovka(year=args.year, max_pages=args.max_pages, chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
