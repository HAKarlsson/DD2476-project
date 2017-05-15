##
# Authors: Martin Hwasser, Dmytro Kalpakchi, Henrik Karlsson
##

import sys
import time
import json
from os import listdir
from os.path import isfile, join
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import deque
from operator import itemgetter
from multiprocessing import Process


#### DEFINING FUNCTIONS ####
def dwell2relevance(dwell_time):
    """
     Calculate the relevance from the dwell time
    """
    if dwell_time < 50:
        return 0
    elif dwell_time < 400:
        return 1
    else:
        return 2


def insert_documents():
    global actions, session_serp, clicks_info
    """
        Insert all the documents that has been recorded
    """
    for cur, nex in zip(actions[:-1], actions[1:]):
        if cur[1] == 'Q':
            continue
        dwell_time = nex[0] - cur[0]
        if dwell_time < 0:
            print("Negative dwell time")

        # handling multiple clicks on the same link
        serp, site = cur[2], cur[3]
        new_relevance = dwell2relevance(dwell_time)
        cur_relevance = clicks_info[serp][site][2]
        if new_relevance > cur_relevance:
            clicks_info[serp][site][2] = new_relevance

    for serp in clicks_info.keys():
        documents = []
        for site, site_info in clicks_info[serp].items():
            pos, domain = site_info[0:2]
            relevance, clicks = site_info[2:]
            documents.append((pos, {
                "site": site,
                "domain": domain,
                "clicks": clicks,
                "relevance": relevance,
            }))
        documents.sort()
        session_serp[serp]['documents'] = list(map(itemgetter(1), documents))

    actions, session_serp = [], []
    clicks_info = dict()


def handle_query(record):
    global session_serp, serps, clicks_info
    """
        Handle a query record
    """
    session_id = int(record[0])
    time_passed = int(record[1])
    serp, query_id = map(int, record[3:5])
    query = record[5].replace(',', ' ')
    clicks_info[serp] = dict()
    is_test = (record[2] == 'T')
    serps.append({
        "_type": "serp",
        "_index": es_index,
        "_parent": session_id,
        'serpId': serp,
        'timePassed': time_passed,
        'query': query,
        'isTest': is_test
    })
    session_serp.append(serps[-1])
    actions.append((time_passed, 'Q'))

    site_domain = [item.split(',') for item in record[6:]]
    for pos, (site, domain) in enumerate(site_domain):
        # 2nd - relevance, 3rd - number of clicks
        clicks_info[serp][int(site)] = [pos, int(domain), 0, 0]


def handle_session(record):
    global sessions
    """
        Handle a session record
    """
    session_id = int(record[0])
    day, user_id = map(int, record[2:])
    sessions.append({
        '_id': session_id,
        "_type": "session",
        "_index": es_index,
        'day': day,
        'user': user_id
    })


def handle_click(record):
    global actions, clicks_info
    """
        Handle a click record
    """
    time_passed = int(record[1])
    serp, site = map(int, record[3:])

    clicks_info[serp][site][3] += 1
    actions.append((time_passed, 'C', serp, site))


def insert_all():
    global sessions, serps
    # insert all the records into elasticsearch
    deque(helpers.parallel_bulk(es, sessions + serps, chunk_size=5000), maxlen=0)
    sessions, serps = [], []


def log_info(start_time, lines_read, file_path):
    # print indexing information
    elapsed = time.time() - start_time
    # lines per second
    lps = lines_read / elapsed
    print("%s > Indexed %d lines, %d lps" % (file_path, lines_read, lps))


def read_file(file_path):
    print("Indexing ", file_path)
    start_time = time.time()
    lines_read = 0
    lines_in_record = 0
    global es
    es = Elasticsearch(timeout=3600)

    with open(file_path) as f:
        for line in f:

            # process the line
            record = line.strip().split('\t')

            if record[1] == 'M':
                # Handle old session
                insert_documents()
                # insert all if we have enough records
                if lines_in_record > 10000:
                    insert_all()
                    log_info(start_time, lines_read, file_path)
                    lines_in_record = 0

                # new session
                handle_session(record)

            elif record[2] == 'Q' or record[2] == 'T':
                # new query
                handle_query(record)

            elif record[2] == 'C':
                # new click
                handle_click(record)

            lines_read += 1
            lines_in_record += 1
    insert_documents()
    insert_all()
    log_info(start_time, lines_read)
    print("%s > DONE! Indexed %d lines" % (file_path, lines_read))


#### PROGRAM START ####

print("python version:", sys.version)

es = Elasticsearch(timeout=3600)

path = sys.argv[1]  # Get the dataset location
es_index = 'yandexFixe'   # set the elasticsearch index

with open('mapping.json') as f:
    if es.indices.exists(index=es_index):
        es.indices.delete(index=es_index)
        print('Deleted index', es_index)
    mappings = json.load(f)
    es.indices.create(index=es_index, body=mappings)
    print('Created index', es_index)

time.sleep(.5)

sessions, serps, actions, session_serp = [], [], [], []
clicks_info = dict()

if isfile(path):
    read_file(path)
else:
    jobs = []
    for file_name in listdir(path):
        part_path = join(path, file_name)
        p = Process(target=read_file, args=(part_path,))
        jobs.append(p)
        p.start()


