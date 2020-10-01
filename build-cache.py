import pprint
import json
import elasticsearch
import os
from datetime import datetime

# SETTINGS
CACHE_INDEX = "es-cache"
AGGS_DIR = "aggs/"

creds = {
    "elasticsearch_host": os.environ["es_host"],
    "elasticsearch_port": os.environ["es_port"],
    "protocol": os.environ["es_proto"],
    "username": os.environ["es_user"],
    "secret_password": os.environ["es_pass"],
}

es = elasticsearch.Elasticsearch(
    hosts=[creds["elasticsearch_host"]],
    http_auth=(creds["username"], creds["secret_password"]),
    scheme=creds["protocol"],
    port=creds["elasticsearch_port"],
    verify_certs=True,
)


def read_json_file(path):
    return json.loads(open(path, "r").read())


# read agg file
agg_files = [f for f in os.listdir(
    AGGS_DIR) if os.path.isfile(os.path.join(AGGS_DIR, f))]

for file in agg_files:
    conf = read_json_file(AGGS_DIR + file)

    # perform filter agg
    filter_terms = es.search(index=conf['index_pattern'], body={
        "size": 0,
        "aggs": {
            "filters": {
                "terms": {
                    "field": conf['filter_term'],
                    "size": conf['filter_terms_size']
                }
            }
        }
    })['aggregations']['filters']['buckets']

    filter_terms = [f for f in filter_terms if f['key'] not in conf['filter_terms_exclude_list']]

    for t in filter_terms:
        # run term specific agg
        cache_agg = es.search(index=conf['index_pattern'], body={
            "size": 0,
            "query": {
                "term": {
                    conf['filter_term']: {
                        "value": t['key']
                    }
                }
            },
            "aggs": conf['aggs']
        })
        
        # persistent cache_agg on es_index
        cache_result = {
            "id": conf['id'] + t['key'],
            "update_timestamp": datetime.now().isoformat(),
            "source": "es-cache-app",
            "index_pattern": conf["index_pattern"],
            "filter_term": conf["filter_term"],
            "filter_terms_size": conf["filter_terms_size"],
            "filter_term_value": t['key'],
            "filter_terms_exclude_list": conf["filter_terms_exclude_list"],
            "search_result": cache_agg
        }
        
        # index doc into ES
        # TODO: should use some kind of bulking mechanism in prod
        print(es.index(CACHE_INDEX, id=cache_result['id'], body=cache_result))


