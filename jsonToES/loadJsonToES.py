import json
from pprint import pprint
from elasticsearch import Elasticsearch

es = Elasticsearch(["localhost:9200"], http_compress=True)
podIds = list()

def detectProvider(rec):
    feedUrl = rec['url']
    rec['provider'] = 'itunes'
    rec['feedURL'] = feedUrl
    try:
        if rec['podcastID'] in podIds:
            raise ValueError(rec['name'])
    except ValueError: 
        print('Oops! duplicated on')
        pprint(rec)
    podIds.append(rec['podcastID'])

    podIds.append(rec['podcastID'])
    if "ssenhosting" in feedUrl or "podbbang" in feedUrl:
        rec['provider'] = 'podbbang'
        rec['feedURL'] = 'http://www.podbbang.com/ch/' + str(rec['podcastID'])
    elif "podty" in feedUrl:
        rec['provider'] = 'podty'
        rec['feedURL'] = 'https://www.podty.me/cast/' + str(rec['podcastID'])
    rec.pop('url')

""" with open('podcastlist.json') as f:
    data = json.load(f)

es.delete_by_query(index='casts', body={"query": {"match_all": {}}})
for rec in data:
    detectProvider(rec)
    rec['cast_episode'] = 'cast'
    pprint(rec)
    es.create(index='casts', doc_type='_doc', id=rec['podcastID'], body=rec) """

casts = es.search(index='casts', body={"size": 100, "query": {"term": {"provider.keyword": "itunes"}}},  filter_path=['hits.hits._id', 'hits.hits._source.feedURL'])
# pprint(casts)
for cast in casts['hits']['hits']:
    pprint(cast)
    cast['feedURL'] = cast['_source']['feedURL']
    cast.pop('_source')
pprint(casts)


