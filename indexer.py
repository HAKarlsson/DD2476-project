import sys
import time
import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from collections import deque
from operator import itemgetter

es = Elasticsearch()


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


def insert_all():
    # insert all the records into elasticsearch
    global sessions, serps, serpitems
    deque(helpers.parallel_bulk(es, sessions+serps, chunk_size=5000), maxlen=0)
    sessions, serps = [], []


def log_info():
    # print indexing information
    elapsed = time.time() - start_time
    # lines per second
    lps = lines_read / elapsed
    print("Indexed %d lines, %d lps" % (lines_read, lps))


start_time = time.time()

# Get program arguments
dataset = sys.argv[1]
es_index = 'yandex'
print("Indexing ", dataset)

print("python version:", sys.version)

sessions, serps = [], []

lines_read = 0
clicks_info = dict()
actions = []
# init next_serp
next_serp = es.count(index='yandex', doc_type='serp')['count']
print("Starting with serp id: ", next_serp)

lines_in_record = 0
with open(dataset) as fp:
    for line in fp:
        lines_read += 1

        # process the line
        record = line.strip().split('\t')
        if record[1] == 'M':

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
                    documents.append({
                        "position": pos,
                        "site": site,
                        "domain": domain,
                        "clicks": clicks,
                        "relevance": relevance,
                    })
                session_serp[serp]['documents'] = documents
            actions = []
            session_serp = []
            clicks_info = dict()

            # new session
            if lines_in_record > 10000:
                insert_all()
                log_info()
                lines_in_record = 0

            session_id = int(record[0])
            day, user_id = map(int, record[2:])
            sessions.append({
                '_id': session_id,
                "_type": "session",
                "_index": es_index,
                'day': day,
                'user': user_id
            })

        elif record[2] == 'Q' or record[2] == 'T':
            # new query
            time_passed = int(record[1])
            serp, query_id = map(int, record[3:5])
            query = record[5].replace(',', ' ')
            clicks_info[serp] = dict()
            is_test = (record[2] == 'T')
            serps.append({
                "_id": next_serp,
                "_type": "serp",
                "_index": es_index,
                "_parent": session_id,
                'serpId': serp,
                'timePassed': time_passed,
                'query': query,
                'isTest': is_test
            })
            session_serp.append(serps[-1])
            next_serp += 1
            actions.append((time_passed, 'Q'))

            site_domain = [item.split(',') for item in record[6:]]
            for pos, (site, domain) in enumerate(site_domain):
                # 2nd - relevance, 3rd - number of clicks
                clicks_info[serp][int(site)] = [pos, int(domain), 0, 0]

        elif record[2] == 'C':
            # new click
            time_passed = int(record[1])
            serp, site = map(int, record[3:])

            clicks_info[serp][site][3] += 1
            actions.append((time_passed, 'C', serp, site))

        lines_in_record += 1


insert_all()
log_info()
print("DONE! Indexed %d lines" % (lines_read))
