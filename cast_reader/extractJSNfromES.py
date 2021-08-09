import json
import re
from elasticsearch import Elasticsearch

es = Elasticsearch(["localhost:9200"])
res = es.search('casts', body={'from': 0, 'size': 500,
                               'query': {'exists': {'field': 'provider'}}
                               })
castlist = list()
for r in res['hits']['hits']:
    cast = r['_source']
    if cast['provider'] == 'podbbang':
        cast['feedURL'] = 'http://pod.ssenhosting.com/ch/%s' % cast['podcastID']
    cast['url'] = cast.pop('feedURL')
    cast['image'] = cast.pop('imageURL')
    castlist.append(cast)
with open('podcastlist.json', 'w', encoding='utf-8') as f:
    print(castlist, file=f)
