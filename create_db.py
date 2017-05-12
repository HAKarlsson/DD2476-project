#!/usr/bin/env python3
##
# Author: Henrik Karlsson
#
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
    """
    CREATE TABLE `session` (
      `session_id` INTEGER,
      `user` INTEGER,
      `day` INTEGER,
      PRIMARY KEY (session_id)
    );
    """,
    """
    CREATE TABLE `query` (
      `query_id` INTEGER,
      `query` TEXT,
      PRIMARY KEY (query_id)
    );
    """,
    """
    CREATE TABLE `sites` (
      `site` INTEGER,
      `domain` INTEGER,
      PRIMARY KEY (site)
    );
    """,
    """
    CREATE TABLE `serp` (
      `serp_id` INTEGER,
      `session_id` INTEGER,
      `serp` INTEGER,
      `time_passed` INTEGER,
      `query_id` INTEGER,
      `is_test` BOOLEAN,
       PRIMARY KEY (serp_id),
       FOREIGN KEY (session_id) REFERENCES session(session_id),
       FOREIGN KEY (query_id) REFERENCES query(query_id)
    );
    """,
    """
    CREATE TABLE `serpitem` (
      `serp_id` INTEGER,
      `position` INTEGER,
      `site` INTEGER,
      PRIMARY KEY (serp_id, position),
      FOREIGN KEY (serp_id) REFERENCES serp(serp_id),
      FOREIGN KEY (site) REFERENCES sites(site)
    );
    """,
    """
    CREATE TABLE `relevance` (
      `serp_id` INTEGER,
      `site` INTEGER,
      `dwell_time` INTEGER,
      FOREIGN KEY (serp_id) REFERENCES serp(serp_id),
      FOREIGN KEY (site) REFERENCES sites(site)
    );
    """,
    """
    CREATE TABLE `clicks` (
      `serp_id` INTEGER,
      `time_passed` INTEGER,
      `site` INTEGER,
      FOREIGN KEY (serp_id) REFERENCES serp(serp_id),
      FOREIGN KEY (site) REFERENCES sites(site)      
    );
    """
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
    command = command.replace('( ', '(').replace(' )', ')')
    c.execute(command)

conn.commit()
conn.close()
