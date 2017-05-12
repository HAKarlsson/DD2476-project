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
      `id` integer,
      `user` integer,
      `day` smallint,
      primary key (id)
    );
    """,
    """
    CREATE TABLE `query` (
      `id` integer,
      `query` text,
      primary key (id)
    );
    """,
    """
    CREATE TABLE `sites` (
      `site` integer,
      `domain` integer,
      primary key (site)
    );
    """,
    """
    CREATE TABLE `serp` (
      `id` integer,
      `session_id` integer,
      `serp` smallint,
      `time_passed` smallint,
      `query_id` integer,
      `is_test` boolean,
       primary key (id),
       FOREIGN KEY (session_id) REFERENCES session(id),
       FOREIGN KEY (query_id) REFERENCES query(id)
    );
    """,
    """
    CREATE TABLE `serpitem` (
      `serp_id` integer,
      `position` smallint,
      `site` integer,
      primary key (serp_id, position),
      FOREIGN KEY (serp_id) REFERENCES serp(id),
      FOREIGN KEY (site) REFERENCES sites(site)
    );
    """,
    """
    CREATE TABLE `relevance` (
      `serp_id` integer,
      `site` integer,
      `dwell_time` integer,
      FOREIGN KEY (serp_id) REFERENCES serp(id),
      FOREIGN KEY (site) REFERENCES sites(site)
    );
    """,
    """
    CREATE TABLE `clicks` (
      `serp_id` integer,
      `time_passed` smallint,
      `site` integer,
      FOREIGN KEY (serp_id) REFERENCES serp(id),
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
