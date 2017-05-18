##
# Authors: Martin Hwasser, Dmytro Kalpakchi, Henrik Karlsson
#
#
# Call with:
#   python3[.6] indexer.py [dataset location]
#   Example:
#       python3 indexer.py dataset/test
#       python3 indexer.py dataset
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


def insert_documents(es):
    """
        Insert all the documents that has been recorded
    """
    for cur, nex in zip(actions[:-1], actions[1:]):
        if cur[1] == 'Q':
            continue
        dwell_time = nex[0] - cur[0]
        # handling multiple clicks on the same link
        serp, site = cur[2], cur[3]
        new_relevance = dwell2relevance(dwell_time)
        cur_relevance = clicks_info[serp][site][2]
        if new_relevance > cur_relevance:
            clicks_info[serp][site][2] = new_relevance

    for action in reversed(actions[::-1]):
        if action[1] == 'C':
            serp, site = action[2], action[3]
            clicks_info[serp][site][2] = 2
            break

    for serp in clicks_info.keys():
        documents = [None] * 10
        for site, site_info in clicks_info[serp].items():
            pos, domain, relevance, clicks = site_info
            documents[pos] = {
                "site": site,
                "domain": domain,
                "clicks": clicks,
                "relevance": relevance,
                "position": pos
            }
        session_serp[serp]['documents'] = documents
    del actions[:]
    del session_serp[:]
    clicks_info.clear()


def handle_query(es, record, session):
    """
        Handle a query record
    """
    time_passed = int(record[1])
    serp = int(record[3])
    query = record[5].replace(',', ' ')
    clicks_info[serp] = dict()
    is_test = (record[2] == 'T')
    serps.append({
        "_routing": session[0],
        "_type": "serp",
        "_index": es_index,
        'serpId': serp,
        'session': session[0],
        'user': session[2],
        'day': session[1],
        'query': query,
        'isTest': is_test
    })
    session_serp.append(serps[-1])
    actions.append((time_passed, 'Q'))
    site_domain = [item.split(',') for item in record[6:]]
    for pos, (site, domain) in enumerate(site_domain):
        # 2nd - relevance, 3rd - number of clicks
        clicks_info[serp][int(site)] = [pos, int(domain), 0, 0]


def handle_session(es, record):
    """
        Handle a session record
    """
    session_id = int(record[0])
    day, user_id = map(int,record[2:])
    return (session_id, day, user_id)

def handle_click(es, record):
    """
        Handle a click record
    """
    time_passed = int(record[1])
    serp = int(record[3])
    site = int(record[4])
    clicks_info[serp][site][3] += 1
    actions.append((time_passed, 'C', serp, site))


def insert_all(es):
    # insert all the records into elasticsearch
    deque(helpers.parallel_bulk(es, serps, chunk_size=5000), maxlen=0)
    del serps[:]


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
    es = Elasticsearch(timeout=3600)
    session = None
    with open(file_path) as f:
        for line in f:
            # process the line
            record = line.rstrip().split('\t')
            if record[1] == 'M':
                # Handle old session
                insert_documents(es)
                # insert all if we have enough records
                if lines_in_record > 10000:
                    insert_all(es)
                    log_info(start_time, lines_read, file_path)
                    lines_in_record = 0
                # new session
                session = handle_session(es, record)
            elif record[2] == 'Q' or record[2] == 'T':
                # new query
                handle_query(es, record, session)
            elif record[2] == 'C':
                # new click
                handle_click(es, record)
            lines_read += 1
            lines_in_record += 1
    insert_documents(es)
    insert_all(es)
    log_info(start_time, lines_read, file_path)
    print("%s > DONE! Indexed %d lines" % (file_path, lines_read))


#### PROGRAM START ####

print("python version:", sys.version)

es = Elasticsearch(timeout=3600, maxsize=30)

dataset_path = None
keep_index = False
args = sys.argv[1:]
i = 0
while i < len(args):
    arg = args[i]
    if arg=='--dataset':
        i += 1
        dataset_path = args[i]
    elif arg == '--keep':
        keep_index = True
    else:
        print("UNKNOWN PARAMETER '%s' at index %d." % (arg, i))
        print("EXITING PROGRAM!")
        sys.exit(-1)
    i += 1

if dataset_path == None:
    print("You have to specify dataset location '--dataset [path to dataset]'. ")
    sys.exit(-1)


es_index = 'yandex'   # set the elasticsearch index
if not keep_index:
    with open('mapping.json') as f:
        if es.indices.exists(index=es_index):
            es.indices.delete(index=es_index)
            print('Deleted index', es_index)
        mappings = json.load(f)
        es.indices.create(index=es_index, body=mappings)
        print('Created index', es_index)

time.sleep(.5)

serps, actions, session_serp = [], [], []
clicks_info = dict()

if isfile(dataset_path):
    read_file(dataset_path)
else:
    jobs = []
    for file_name in listdir(dataset_path):
        part_path = join(dataset_path, file_name)
        p = Process(target=read_file, args=(part_path,))
        jobs.append(p)
        p.start()
