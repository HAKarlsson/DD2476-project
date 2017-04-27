#!/usr/bin/python3
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

data = sys.argv[1]
database = sys.argv[2]

conn = sqlite3.connect(database)
c = conn.cursor()

print("sqlite3 version:", sqlite3.sqlite_version)
print("python version:", sys.version)
i = 0
with open(data) as f:
    for line in f:
        i += 1
        line = line.strip().split('\t')
        sessionID = line[0]
        if line[1] == 'M':
            day = line[2]
            userID = line[3]
            c.execute("INSERT INTO session VALUES (%s,%s,%s)" % (sessionID, day, userID))

        elif line[2] == 'Q' or line[2] == 'T':
            time_passed = line[1]
            query_type = line[2]
            serp = line[3]
            query_id = line[4]
            query = line[5]
            c.execute("INSERT INTO search VALUES (%s,%s,%s,%s,'%s')" %
                      (sessionID, serp, time_passed, query_id, query_type))
            sites = []
            for position, siteDomain in enumerate(line[6:]):
                site, domain = siteDomain.split(',')
                c.execute("INSERT OR IGNORE INTO site2domain VALUES (%s,%s)" % (site, domain))
                sites.append(site)
            c.execute("INSERT OR IGNORE INTO query VALUES (%s,'%s',%s)" % (query_id,query,','.join(sites)))
        elif line[2] == 'C':
            time_passed = line[1]
            serp = line[3]
            site = line[4]
            c.execute("INSERT INTO click VALUES (%s,%s,%s,%s)" %
                      (sessionID, serp, time_passed, site))

        if i % 100000 == 0 and i > 0:
            print("Indexed: %d lines" % i)
            conn.commit()
print("Indexed: %d lines" % i)
conn.commit()
conn.close()
