# DD2476-project

Yandex Personalized Web Search Challange: https://www.kaggle.com/c/yandex-personalized-web-search-challenge

### How to use

*  Install elasticsearch and kibana.
*  Download the train & test data and put them in a folder called dataset in the same directory as the scripts. Then call `./index.sh`

### Elasticsearch schema: 
See `mapping.json`

### Elasticsearch LTR Plugin:

Installing the plugin:
```
./bin/elasticsearch-plugin install http://es-learn-to-rank.labs.o19s.com/ltr-query-0.1.1-es5.4.0.zip
```

Add lines in `elasticsearch.yml` to avoid error:
```
script.max_size_in_bytes: 10000000
```