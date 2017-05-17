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
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")

##
# Extract features from elasticsearch
##


def get_session(day):
    """
    Get sessions from a specific day, this function can take a while.
    We should probably save the data from this into a file and then read from the file
    """
    page = template_query(id="day2sessions",
                             params={"day": day})
    # returns list of (session_id, number of serps in session)
    buckets = page['aggregations']['sessions']['buckets']
    return [(b['key'], b['doc_count']) for b in buckets]


def get_serp(session_id):
    """
    Get serps for a batch of sessions
    """
    res = template_query(id="session2serps", params={"session_id": session_id})
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


def dump2ranklib_file(qid, labels, info, features):
    output = ""
    for pos in range(10):
        output += "%d qid:%d" % (labels[pos], qid)
        for num, feature in enumerate(features[pos, :]):
            output += " %d:%.3f" % (num, feature)
        output += " # %d\n" % info[pos]
    print(output, end='')


def template_query(id, params):
    res = es.search_template(index=es_index, body={
        "inline": es.get_template(id=id)["template"],
        "params": params})
    return res


def put_templates():
    for file_name in listdir("search_templates"):
        part_path = join("search_templates", file_name)
        file_name_no_ext = file_name.split(".")[0]
        with open(part_path) as f:
            body = json.load(f)
            es.put_template(id=file_name_no_ext, body=body)


def log_info(start_time, sessions_processed):
    # print indexing information
    elapsed = time.time() - start_time
    # lines per second
    sps = sessions_processed / elapsed
    logging.info("Processed %d sessions, %.2f sps" % (sessions_processed, sps))


"""
Get sessions
Create dictionaries for session
fill dictionaries with serp
session -> serp
"""

if len(sys.argv) < 2:
    logging.error("""
        You have to provide it with day argument like
           python feature_extraction.py 3
           python feature_extraction.py 3:18
        """)
    sys.exit(1)

day_range = sys.argv[1].split(':')
start = int(day_range[0])
end = start if len(day_range) == 1 else int(day_range[1])

isTest = False
if len(sys.argv) > 2 and sys.argv[2] == 'test':
    isTest = True

# Create an elasticsearch client
es = Elasticsearch(timeout=3600)
# load templates to node
put_templates()

es_index = 'yandex'
sessions_processed = 0
start_time = time.time()
for day in range(start, end + 1):
    sessions = get_session(day)
    sessions_processed = 0
    start_time = time.time()
    for (session_id, serp_count) in sessions:
        serps = get_serp(session_id)
        labels, info = get_labels(serps[-1])
        if np.sum(labels) > 0 or isTest:
            features = get_features(serps)
            dump2ranklib_file(session_id, labels, info, features)
        sessions_processed += 1
        log_info(start_time, sessions_processed)
