#!/bin/bash
./create_db.py yandex.db
./indexer.py dataset/test yandex.db
./indexer.py dataset/train yandex.db
