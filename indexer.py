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


def dwell2relevance(dwell_time):
    if dwell_time < 50:
        return 0
    elif dwell_time < 400:
        return 1
    else:
        return 2
        
        
def insert_all():
    global sessions, serps, serpitems, queries
    cursor.executemany("INSERT INTO session VALUES (?, ?, ?)", sessions)
    cursor.executemany("INSERT OR IGNORE INTO query VALUES (?, ?)", queries)
    cursor.executemany("INSERT INTO serp VALUES (?, ?, ?, ?, ?, ?)", serps)
    cursor.executemany("INSERT INTO serpitem VALUES (?, ?, ?, ?, ?, ?)", serpitems)
    conn.commit()
    sessions, serps, serpitems, queries = [], [], [], []


def log_info():
    elapsed = time.time() - start_time
    # lines per second
    lps = lines_read / elapsed
    print("Indexed %d lines, %d lps"% (lines_read, lps))


start_time = time.time()

# Get program arguments, dataset and database locations
dataset = sys.argv[1]
database = sys.argv[2]
print("Indexing ", dataset)

# Create a connection to the database
conn = sqlite3.connect(database)
cursor = conn.cursor()

cursor.execute("PRAGMA synchronous=OFF")
cursor.execute("PRAGMA check_foreign_key=False")

print("sqlite3 version:", sqlite3.sqlite_version)
print("python version:", sys.version)

sessions, serps, serpitems, queries = [], [], [], []

# init serp_id
cursor.execute('SELECT max(serp_id) FROM serp')
cursor_res = cursor.fetchone()[0]
serp_id = -1 if cursor_res == None else cursor_res
    
lines_read = 0
clicks_info = dict()
actions = []
with open(dataset) as fp:
    for line in fp:
        
        # process the line
        record = line.strip().split('\t')
        if record[1] == 'M':
            session_id = int(record[0])
            day, user_id = record[2:]
            sessions.append((session_id, user_id, day))
            
            for cur, nex in zip(actions[:-1], actions[1:]):
                if cur[1] == 'Q':
                    continue
                dwell_time = int(nex[0]) - int(cur[0])
                if dwell_time < 0: 
                    print("Negative dwell time")
                
                # handling multiple clicks on the same link
                serp = cur[2]
                site = cur[3]
                new_relevance = dwell2relevance(dwell_time)
                curr_relevance = clicks_info[serp][site][3]
                if new_relevance > curr_relevance:
                    clicks_info[serp][site][3] = new_relevance
                    
            for serp in clicks_info.keys():
                for site, site_info in clicks_info[serp].items():
                    serp_id, pos, domain = site_info[0:3]
                    relevance, clicks = site_info[3:]
                    serpitems.append((serp_id, pos, site, domain, clicks, relevance))
            actions = []
            clicks_info = dict()
        elif record[2] == 'Q' or record[2] == 'T':
            serp_id += 1
            time_passed = record[1]
            serp, query_id, query = record[3:6]
            clicks_info[serp] = dict()
            is_test = (record[2] == 'T')
            
            queries.append((query_id, query))
            serps.append((serp_id, session_id, serp, time_passed, query_id, is_test))
            actions.append((time_passed, 'Q'))
            
            site_domain = [item.split(',') for item in record[6:]]
            for pos, (site, domain) in enumerate(site_domain):
                # 3nd - relevance, 4rd - number of clicks
                clicks_info[serp][site] = [serp_id, pos, domain, 0, 0]
                
        elif record[2] == 'C':
            time_passed = record[1]
            serp, site = record[3:]
            clicks_info[serp][site][4] += 1
            actions.append((time_passed, 'C', serp, site))
        
        lines_read += 1
        if (lines_read % 50000) == 0:
            insert_all()
            log_info()
            
insert_all()
log_info()
conn.close()
print("DONE! Indexed %d lines"% (lines_read))