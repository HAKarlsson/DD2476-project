#!/usr/bin/env bash
# Author: Henrik Karlsson
#
rm -f yandex.db
./create_db.py yandex.db
./indexer.py dataset/test yandex.db
./indexer.py dataset/train yandex.db
