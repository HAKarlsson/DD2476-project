#!/usr/bin/python3
##
# INSTRUCTION FOR *LINUX*
#
# ./create_db.py DATABASE_LOCATION
#
# example:
#   ./create_db.py my_database.db
#
# Creates a database for indexer.py
##
import sqlite3
import sys

commands = (
    "CREATE TABLE site2domain(\
        site INTEGER PRIMARY KEY ASC,\
        domain INTEGER\
    );",
    # site -> domain
    "CREATE TABLE session(\
        session_id INTEGER PRIMARY KEY ASC,\
        day INTEGER,\
        user INTEGER\
    );",
    # session_id -> day, user
    "CREATE TABLE search(\
        session_id INTEGER,\
        serp INTEGER,\
        time_passed INTEGER,\
        query_id INTEGER,\
        query_type TEXT,\
        PRIMARY KEY(session_id, serp)\
    );",
    # session_id, serp -> time_passed, query_id, query_type
    "CREATE TABLE query(\
        query_id INTEGER ASC,\
        query TEXT,\
        site0 INTEGER,\
        site1 INTEGER,\
        site2 INTEGER,\
        site3 INTEGER,\
        site4 INTEGER,\
        site5 INTEGER,\
        site6 INTEGER,\
        site7 INTEGER,\
        site8 INTEGER,\
        site9 INTEGER,\
        PRIMARY KEY(query_id)\
    );",
    # query_id -> query, site[0-9]
    "CREATE TABLE click(\
        session_id INTEGER ASC,\
        serp INTEGER ASC,\
        time_passed INTEGER,\
        site INTEGER\
    );"
    # session_id, serp -> time_passed, site
)


print("sqlite3 version:", sqlite3.sqlite_version)
print("python version:", sys.version)

database = sys.argv[1]

conn = sqlite3.connect(database)
c = conn.cursor()
for i, command in enumerate(commands):
    print("executing command", i)
    # remove redundant whitespaces, for when viewing sql schema
    command = ' '.join(command.split())
    command = command.replace('( ','(').replace(' )',')')
    c.execute(command)

conn.commit()
conn.close()
