#!/usr/bin/env python3
##
# INSTRUCTION FOR *LINUX*
#
# ./indexer.py DATASET DATABASE
#
# example:
#   ./indexer.py dataset/train my_database.db
#
# Reads yandex data and indexes it to a database.
#
# Expects:
# Session:
#   0         1 2   3
#   sessionID M day userID
# Query:
#   0         1            2   3    4       5     6-9
#   sessionID time_passed [QT] SERP queryID query site,domain
# Click:
#   0         1           2 3    4
#   sessionID time_passed C SERP site
# with tab as separators
##
import sqlite3
import sys
import time

tstart = time.time()

data = sys.argv[1]
database = sys.argv[2]

conn = sqlite3.connect(database)
c = conn.cursor()

print("sqlite3 version:", sqlite3.sqlite_version)
print("python version:", sys.version)
i = 0
start = time.time()

to_sessions = []
to_site2domain = []
to_query = []
to_click = []
to_serp = []

c.execute('PRAGMA synchronous = OFF')


def insertAll():
    global to_sessions, to_site2domain, to_query, to_click, to_serp
    c.executemany("INSERT INTO session VALUES (?,?,?)", to_sessions)
    c.executemany("INSERT OR IGNORE INTO site2domain VALUES (?,?)", to_site2domain)
    c.executemany("INSERT OR IGNORE INTO query VALUES (?,?)", to_query)
    c.executemany("INSERT INTO click VALUES (?,?,?,?)", to_click)
    c.executemany("INSERT INTO serp VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", to_serp)
    conn.commit()
    to_sessions = []
    to_site2domain = []
    to_query = []
    to_click = []
    to_serp = []

with open(data) as f:
    for line in f:
        i += 1
        line = line.strip().split('\t')
        session_id = line[0]
        if line[1] == 'M':
            day = line[2]
            user = line[3]
            to_sessions.append((session_id, day, user))

        elif line[2] == 'Q' or line[2] == 'T':
            time_passed = line[1]
            query_type = line[2]
            serp = line[3]
            query_id = line[4]
            query = line[5]

            to_query.append((query_id, query))

            sites = []
            for position, siteDomain in enumerate(line[6:]):
                site, domain = siteDomain.split(',')
                to_site2domain.append((site, domain))
                sites.append(int(site))
            to_serp.append((session_id, serp, time_passed, query_type, query_id,
                sites[0], sites[1], sites[2], sites[3], sites[4],
                sites[5], sites[6], sites[7], sites[8], sites[9]))
        elif line[2] == 'C':
            time_passed = line[1]
            serp = line[3]
            site = line[4]
            to_click.append((session_id, serp, time_passed, site))

        if i % 50000 == 0 and i > 0:
            insertAll()
            elapsed = time.time() - start
            telapsed = time.time() - tstart
            start = time.time()
            print("Indexed: %d lines, total time: %.2f, time: %.2f" % (i, telapsed, elapsed))
insertAll()
conn.close()

telapsed = time.time() - tstart

print("Indexed: %d lines, total time: %.2f" % (i, telapsed))
