from elasticsearch import Elasticsearch
from pprint import pprint
import numpy as np

##
# Extract features from elasticsearch
##

# Create an elasticsearch client
es = Elasticsearch()
es_index = 'yandex'
#
# step 1, get a serp
#


def get_session(day, size):
    """
    Get sessions from a specific day
    """
    query = \
        {
            "size": size,
            "_source": True,
            "query": {
                "match": {
                    "day": day
                }
            }
        }
    page = es.search(index=es_index, doc_type='session',
                     body=query, scroll="10m")
    sid = page["_scroll_id"]
    scroll_size = page['hits']['total']
    yield page['hits']['hits']
    while scroll_size > 0:
        page = es.scroll(scroll_id=sid, scroll='10m')
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
        yield page['hits']['hits']


def get_serp(session):
    """
    Get serps for a batch of sessions
    """
    query = \
        {
            "size": 100,
            "sort": [
                {"serpId": "asc"}
            ],
            "query": {
                "parent_id": {
                    "type": "serp",
                    "id": session["_id"]
                }
            }
        }

    res = es.search(index=es_index, doc_type='serp',
                    body=query, filter_path=['**._source'])
    return res['hits']['hits']


def get_features(serps):
    """
    Extracted features are:
      1. Unpersonalized rank
      2. Unpersonalized rank scaled with exp
      3. Total number of clicks for the same query
      4. Total relevance of all clicks for the same query
      3. hit/miss/skip?
    """
    label_docs = serps[-1]["_source"]["documents"]
    session_history = [serp["_source"] for serp in serps[:-1]]
    num_features = 4
    features = np.zeros((10,num_features))
    query = serps[-1]["_source"]["query"]
    for pos, doc in enumerate(label_docs):
        site = doc['site']
        domain = doc['domain']
        features[pos,0] = pos
        features[pos,1] = np.exp(-pos)
        
        for serp in session_history:
            if serp["query"] == query:
                for doc in serp["documents"]:
                    features[pos,2] += doc["clicks"]
                    features[pos,3] += doc["relevance"]
    return features


def get_labels(serp):
    """
    Get the relevance labels from the serp
    """
    labels = []
    for doc in serp['_source']['documents']:
        labels.append(doc['relevance'])
    return labels


def dump2ranklib_file(qid, labels, features):
    with open("features.rkb", "a") as f:
        for pos in range(10):
            line = "%d qid:%d" % (labels[pos], qid)
            for num, feature in enumerate(features[pos,:]):
                line += " %d:%.3f" % (num, feature)
            f.write(line + "\n")
    

"""
Get sessions
Create dictionaries for session
fill dictionaries with serp
session -> serp
"""
qid = 0
for sessions in get_session(3, 100):
    for session in sessions:
        serps = get_serp(session)
        features = get_features(serps)
        labels = get_labels(serps[-1])
        """
         print to RankLib file
        """
        dump2ranklib_file(qid, labels, features)    
        qid += 1