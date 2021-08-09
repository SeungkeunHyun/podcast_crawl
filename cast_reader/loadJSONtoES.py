import json
import re
from elasticsearch import Elasticsearch

with open('podcastlist.json', 'r+', encoding='utf-8') as f:
    casts = json.load(f)
es = Elasticsearch(hosts=["localhost:9200"])
print(es)
for cast in casts:
    cast['feedURL'] = cast.pop('url')
    cast['imageURL'] = cast.pop('image')
    if cast['provider'] == 'podbbang':
        cast['feedURL'] = 'http://www.podbbang.com/ch/%s' % cast['podcastID']
    res = es.index(index='casts', doc_type='_doc',
                   id=str(cast['podcastID']), body=cast)
    print(res, cast)
