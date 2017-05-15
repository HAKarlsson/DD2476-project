# DD2476-project

Yandex Personalized Web Search Challange: https://www.kaggle.com/c/yandex-personalized-web-search-challenge

### How to use

*  Install elasticsearch and kibana.
*  Create an index by copy pasting the `PUT yandex\n{...` from  `mapping.json` to the kibana console window (Dev tools) or use `curl -X PUT ...` 
*  Download the train & test data and put them in a folder called dataset in the same directory as the scipts. Then call `./index.sh`

### Elasticsearch schema: 
See `mapping.json`

