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
import numpy as np
import json
import sys
import time
import logging
import multiprocessing as mp

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")

##
# Extract features from elasticsearch
##


def get_session(day):
    """
    Get sessions from a specific day, this function can take a while.
    We should probably save the data from this into a file and then read from the file
    """
    query = \
        {
            "size": 0,
            "query": {
                "match": {
                    "day": day
                }
            },
            "aggs": {
                "sessions": {
                    "terms": {
                        "field": "session",
                        "size": 4000000
                    }
                }
            }
        }
    page = es.search(index=es_index, doc_type='serp', body=query,
                     filter_path=['**.key', '**.doc_count'])
    # returns list of (session_id, number of serps in session)
    return [(bucket['key'], bucket['doc_count']) for bucket in page['aggregations']['sessions']['buckets']]


def get_serp(session_id):
    """
    Get serps for a batch of sessions
    """
    query = \
        {
            "size": 300,
            "sort": [
                {"serpId": "asc"}
            ],
            "query": {
                "match": {
                    "session": session_id
                }
            }
        }

    res = es.search(index=es_index, doc_type='serp',
                    body=query, filter_path=['**._source'])
    serps = [r['_source'] for r in res['hits']['hits']]
    return serps


def get_features(serps):
    """
    Extracted features are:
      0. Unpersonalized rank
      1. Unpersonalized rank scaled with exp
      2. Total number of clicks for the same query in same session
      3. Total relevance of all clicks for the same query in same session
      4. Total clicks in same session same site
      5. Total relevance in same session same site
      6. Total displayed in same session same site
      7. Total clicks in same session same domain
      8. Total relevance in same session same domain
      9. Total displayed in same session same domain
      3. hit/miss/skip? (see dataiku sec 4.2.2)
    """
    label_docs = serps[-1]["documents"]
    session_history = [serp for serp in serps[:-1]]
    num_features = 10
    features = np.zeros((10, num_features))
    query = serps[-1]["query"]

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
                    features[pos, 4] += doc['clicks']
                    features[pos, 5] += doc['relevance']
                    features[pos, 6] += 1
                if doc['domain'] == domain:
                    features[pos, 7] += doc['clicks']
                    features[pos, 8] += doc['relevance']
                    features[pos, 9] += 1

    return features


def get_labels(serp):
    """
    Get the relevance labels from the serp
    """
    labels = []
    for doc in serp['documents']:
        labels.append(doc['relevance'])
    return labels


def dump2ranklib_file(qid, labels, features):
    output = ""
    for pos in range(10):
        output += "%d qid:%d" % (labels[pos], qid)
        for num, feature in enumerate(features[pos, :]):
            output += " %d:%.3f" % (num, feature)
        output += "\n"
    print(output, end='')


def template_query():
    with open("search_templates/temp.mustache") as f:
        body = json.load(f)

    es.put_template(id="temp", body=body)
    res = es.search_template(index=es_index, body={
        "inline": es.get_template(id="temp")["template"],
        "params": {
            "day": 5,
            "user": 10
        }})
    return res


def log_info(start_time, session_processed):
    # print indexing information
    elapsed = time.time() - start_time
    # lines per second
    sps = session_processed / elapsed
    logging.info("Processed %d sessions, %.2f sps" % (session_processed, sps))


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


# Create an elasticsearch client
es = Elasticsearch(timeout=3600)
es_index = 'yandex'
start_time = time.time()
sessions_processed = 0
qid = 0
for day in range(start, end + 1):
    for (session_id, serp_count) in get_session(day):
        if(serp_count < 3):
            continue
        serps = get_serp(session_id)[:-1]
        features = get_features(serps)
        labels = get_labels(serps[-1])
        dump2ranklib_file(qid, labels, features)
        qid += 1

"""
days [1-24] -> history dataset
days [25-27] -> training set
days [28-30] -> test set

session 0
serp0
serp1
session 1 history
serp0
serp1
serp2
serp3 <- for prediction 
"""
