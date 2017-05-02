#!/usr/bin/env python3
##
# Author: Henrik Karlsson
#
# INSTRUCTION FOR *LINUX*
# ./click2relevance.py DATABASE
#
# Adds a table of relevance for a site given a session and its serp.
#   relevance(session_id, serp, relevance, site)
#
# ** READ THIS **
# Relevance is based on dwell time. Dwell time is the time passed between 
# the click on the document and the next click or the next query.
#
# If a document was clicked several times then the maximum dwell time is 
# used to label the document's relevance. This script does NOT consider 
# multiple clicks but adds one relevance entry for EVERY click that can 
# have it's dwell time computed. To get the click with the maximum dwell
# time, use an SQL query.
# 
##

import sqlite3
import sys

def CalcRelevance(dwell_time):
    if dwell_time < 50:
        return 0
    elif dwell_time < 400:
        return 1
    return 2

database = sys.argv[1]

conn = sqlite3.connect(database)
c = conn.cursor()

c.execute("CREATE TABLE relevance(session_id INTEGER ASC, serp INTEGER ASC, relevance INTEGER, site INTEGER);")

query = \
'SELECT session_id, serp, time_passed, query_id, 0 AS type \
FROM serp \
UNION ALL \
SELECT session_id, serp, time_passed, site, 1 \
FROM click \
ORDER BY session_id, time_passed, type;'

    
# Should be 132,041,542 lines in total to process
num_of_lines = 132041542
# curr_row
curr_row = (-1,0,0,0,0)
# relevance labels to be added
to_relevance = []
# number of lines processed
i = 0
for next_row in c.execute(query):
    # each row consists of
    #   session_id, serp, time_passed, query_id, type (query or click)
    if curr_row[0] == next_row[0] and curr_row[4] == 1:
        # Calculate the relevance
        dwell_time = next_row[2] - curr_row[2]
        relevance = CalcRelevance(dwell_time)
        to_relevance.append((curr_row[0],curr_row[1],relevance,curr_row[3]))

    curr_row = next_row
    i+=1
    if (i % 1000000) == 0:
        print("Processed %d million lines, %.2f%% done" % (i//1000000, i/num_of_lines * 100))
        print("  %d relevance labels found" % len(to_relevance))


print("Inserting data.")
c.executemany("INSERT INTO relevance VALUES (?,?,?,?)", to_relevance)
print("Done. Processed %d lines" % i)
print("  %d relevance labels were inserted" % len(to_relevance))
conn.commit()
conn.close()
