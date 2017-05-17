#
# Example usage:
#    python feature_extraction.py 2 > features.rkb
#           for getting features for day 2 only
#
#    python feature_extraction.py 2:17 > features.rkb
#           for getting features between day 2 and 17 inclusively
#

from elasticsearch import Elasticsearch
from pprint import pprint
from os import listdir
from os.path import isfile, join
import numpy as np
import json
import sys
import time
import multiprocessing as mp
import queue

##
# Extract features from elasticsearch
##


def get_session(es, day):
    """
    Get sessions from a specific day, this function can take a while.
    We should probably save the data from this into a file and then read from the file
    """
    page = template_query(es, id="day2sessions",
                          params={"day": day})
    # returns list of (session_id, number of serps in session)
    buckets = page['aggregations']['sessions']['buckets']
    return [(b['key'], b['doc_count']) for b in buckets]


def get_serp(session_id, es):
    """
    Get serps for a batch of sessions
    """
    res = template_query(es, id="session2serps", params={
                         "session_id": session_id})
    serps = [r['_source'] for r in res['hits']['hits']]
    return serps


def get_features(serps):
    """
    Extracted features are:
      0. Unpersonalized rank
      1. Unpersonalized rank scaled with exp
      2. Total number of clicks for the same query in same session
      3. Total relevance of all clicks for the same query in same session
      4. Avg clicks in same session same site
      5. Avg relevance in same session same site
      6. Avg displayed in same session same site
      7. Avg clicks in same session same domain
      8. Avg relevance in same session same domain
      9. Avg displayed in same session same domain
      3. hit/miss/skip? (see dataiku sec 4.2.2)
    """
    label_docs = serps[-1]["documents"]
    session_history = [serp for serp in serps[:-1]]
    num_features = 10
    features = np.zeros((10, num_features))
    query = serps[-1]["query"]
    user = serps[-1]['user']
    day = serps[-1]['day']

    for pos, doc in enumerate(label_docs):
        site = doc['site']
        domain = doc['domain']
        features[pos, 0] = pos
        features[pos, 1] = np.exp(-pos)

        for serp in session_history:
            if serp["query"] == query:
                doc = serp["documents"][pos]
                features[pos, 2] += doc["clicks"]
                features[pos, 3] += doc["relevance"]

            for doc in serp["documents"]:
                if doc['site'] == site:
                    features[pos, 4] += doc['clicks'] / len(serps)
                    features[pos, 5] += doc['relevance'] / len(serps)
                    features[pos, 6] += 1 / len(serps)
                if doc['domain'] == domain:
                    features[pos, 7] += doc['clicks'] / len(serps)
                    features[pos, 8] += doc['relevance'] / len(serps)
                    features[pos, 9] += 1 / len(serps)

    return features


def get_labels(serp):
    """
    Get the relevance labels from the serp
    """
    labels = []
    info = []
    for doc in serp['documents']:
        labels.append(doc['relevance'])
        info.append(doc['site'])
    return labels, info


def dump2ranklib(labels, info, features, session_id):
    output = ""
    for pos in range(10):
        output += "%d qid:%d" % (labels[pos], session_id)
        for num, feature in enumerate(features[pos, :]):
            output += " %d:%.3f" % (num, feature)
        output += " # %d\n" % info[pos]
    return output


def template_query(es, id, params):
    res = es.search_template(index=es_index, body={
        "inline": templates[id],
        "params": params})
    return res


def get_templates():
    templates = dict()
    for file_name in listdir("search_templates"):
        part_path = join("search_templates", file_name)
        file_no_ext = file_name.split(".")[0]
        with open(part_path) as fp:
            body = json.load(fp)
            es.put_template(id=file_no_ext, body=body)
        templates[file_no_ext] = es.get_template(id=file_no_ext)["template"]
    return templates


def log_info():
    global start_time, sessions_processed, total_processed
    # print indexing information
    elapsed = time.time() - start_time
    if elapsed >= 2:
        # lines per second
        sps = sessions_processed / elapsed
        print("Processed %d sessions, %.2f sps" % (total_processed, sps))
        start_time = time.time()
        sessions_processed = 0


def producer(output_queue, session_queue):
    es = Elasticsearch(timeout=3600)
    while True:
        try:
            item = session_queue.get()
            if item is None:
                break
            session_id = item
            serps = get_serp(session_id, es)
            labels, info = get_labels(serps[-1])
            if np.sum(labels) > 0 or isTest:
                features = get_features(serps)
                output_queue.put((labels, info, features, session_id))
        except queue.Empty:
            pass


def consumer(output_queue, output_file):
    global start_time, sessions_processed, total_processed
    start_time = time.time()
    sessions_processed = 0
    total_processed = 0
    with open(output_file, 'w') as fp:
        while True:
            try:
                item = output_queue.get()
                if item is None:
                    break
                fp.write(dump2ranklib(*item))
                sessions_processed += 1
                total_processed += 1
                log_info()
            except queue.Empty:
                pass
    fp.close()

"""
Get sessions
Create dictionaries for session
fill dictionaries with serp
session -> serp
"""

# Handle program arguments
days = None
isTest = False
output_file = None

args = sys.argv[1:]
i = 0
while i < len(args):
    arg = args[i]
    if arg == '--days':
        i += 1
        days = tuple(args[i].split(':'))
        if len(days) == 1:
            days += days
        print(days)
        days = tuple(map(int, days))
    elif arg == '--output':
        i += 1
        output_file = args[i]
    elif arg == '--test':
        isTest = True
    else:
        print("UNKNOWN PARAMETER '%s' at index %d." % (arg, i))
        print("EXITING PROGRAM!")
        sys.exit(-1)
    i += 1

if output_file == None:
    print("Specify output file '--output [filename]'")
    sys.exit(-1)
elif days == None:
    print("Specify days to create features on '--days [day]' or '--day [first day]:[last day]'")
    sys.exit(-1)

# Create an elasticsearch client
es = Elasticsearch(timeout=3600)
# load templates to node
templates = get_templates()


es_index = 'yandex'
start_time = time.time()


output_queue = mp.Queue()
session_queue = mp.Queue()

cons = mp.Process(name='cons', target=consumer,
                  args=(output_queue, output_file,))
jobs = []
for i in range(mp.cpu_count() * 2):
    job = mp.Process(name='prod%d' % i, target=producer,
                     args=(output_queue, session_queue,))
    jobs.append(job)
    job.start()

cons.start()

for day in range(days[0], days[1] + 1):
    sessions = get_session(es, day)
    for (session_id, serp_count) in sessions:
        session_queue.put(session_id)

for i in range(len(jobs)):
    session_queue.put(None)

for job in jobs:
    job.join()
output_queue.put(None)
cons.join()
