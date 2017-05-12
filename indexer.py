#!/usr/bin/env python3
##
# Author: Henrik Karlsson, Dmytro Kalpakchi
#
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
to_sites = []
to_serpitem = []
to_relevance = []
to_query = []
to_click = []
to_serp = []

c.execute('PRAGMA synchronous = OFF')


def insertAll():
    # Insert all db entries
    global to_sessions, to_sites, to_query, to_click, to_serp, to_serpitem, to_relevance
    c.executemany("INSERT INTO session VALUES (?,?,?)", to_sessions)
    c.executemany("INSERT OR IGNORE INTO sites VALUES (?,?)", to_sites)
    c.executemany("INSERT OR IGNORE INTO query VALUES (?,?)", to_query)
    c.executemany("INSERT INTO serp VALUES (?,?,?,?,?,?)", to_serp)
    c.executemany("INSERT INTO serpitem VALUES (?,?,?)", to_serpitem)
    c.executemany("INSERT INTO relevance VALUES (?,?,?)", to_relevance)
    c.executemany("INSERT INTO clicks VALUES (?,?,?)", to_click)
    conn.commit()
    to_sessions = []
    to_sites = []
    to_serpitem = []
    to_relevance = []
    to_query = []
    to_click = []
    to_serp = []


print("\nIndexing %s"% data)

# Find starting point for serp_id
c.execute("SELECT max(id) FROM serp")
max_id = c.fetchone()[0]
if max_id == None:
    serp_id = -1
else:
    serp_id = max_id
print("SERP id starting at %d\n"% (serp_id + 1))


time_passed = []

with open(data) as f:
    for line in f:
        i += 1
        line = line.strip().split('\t')
        session_id = int(line[0])
        if line[1] == 'M':
            # New session
            day = line[2]
            user = line[3]
            to_sessions.append((session_id, day, user))
            # can we say that each metadata line indicates the start of new session? yes
            
            for curr, nex in zip(time_passed[:-1],time_passed[1:]):
                if curr[2] == 'Q':
                    continue
                # Create relevance entry
                dwell_time = nex[1] - curr[1]
                if dwell_time < 0:
                    print("ERROR")
                to_relevance.append((curr[0], curr[-1], dwell_time))
            time_passed = []
            
        elif line[2] == 'Q' or line[2] == 'T':
            # New query
            serp_id += 1
            serp = line[3]
            q_time_passed = int(line[1])
            query_id = line[4]
            is_test = (line[2] == 'T')
            query = line[5]
            
            time_passed.append((serp_id, q_time_passed,'Q', None)) # why tuple? We want to diffirentiate between query time and click time
            
            # Create a new SERP
            to_serp.append((serp_id, session_id, serp, q_time_passed, query_id, is_test))
            
            # Create a new query
            to_query.append((query_id, query))

            # Add new sites if they do not exist
            sites = [siteDomain.split(',') for siteDomain in line[6:]]
            for pos, siteDomain in enumerate(sites):
                to_sites.append(siteDomain)
                to_serpitem.append((serp_id, pos, siteDomain[0]))
            
        elif line[2] == 'C':
            # New click
            c_time_passed = int(line[1])
            site = line[4]
            to_click.append((serp_id, c_time_passed, site))
            time_passed.append((serp_id,c_time_passed, 'C', site))
            

        if i % 50000 == 0 and i > 0:
            # Insert all rows
            insertAll()
            # Print log
            elapsed = time.time() - start
            telapsed = time.time() - tstart
            start = time.time()
            print("Indexed: %d lines, total time: %.2f, time: %.2f" %
                  (i, telapsed, elapsed))

# Make sure everyting is inserted.
insertAll()
conn.close()

telapsed = time.time() - tstart

print("Indexed: %d lines, total time: %.2f" % (i, telapsed))
