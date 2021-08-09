#!/usr/bin/python3
import elasticsearch
import re
import json

class CastUpdater:
    name = 'CastUpdater'
    es = elasticsearch.Elasticsearch(["localhost:9200"])    
    casts = []
    def __init__(self):
        res = self.es.search(index="casts", size=500)
        for c in res['hits']['hits']:
            cast = c['_source']
            self.casts.append(cast)
            self.updateCast(cast['podcastID'])
        print(self.casts)
        pass
    def log(self, msg):
        print(msg)
    def updateCast(self, castID):
        qry = {
            "size": 0,
            "query": {
                "term": {
                    "cast_episode.keyword": {
                        "value": castID
                    }
                }
            },
            "aggs": {
                "lastPubdate": {
                    "max": {
                        "field": "pubDate"
                    }
                },
                "ep_count": {
                    "value_count": {
                        "field": "mediaURL.keyword"
                    }
                }
            }
        }
        qres = self.es.search(index='episodes', body=qry)
        epCount = qres['aggregations']['ep_count']['value']
        epUpdateAt = qres['aggregations']['lastPubdate']['value_as_string']
        self.log("%s has %d episodes updated at %s" % (castID, epCount, epUpdateAt))
        self.log({'lastPubAt': epUpdateAt, 'episodes': epCount})
        res = self.es.update(index='casts', id=castID, body={'doc': {'lastPubAt': epUpdateAt, 'episodes': epCount}})
        self.log(res)

cu = CastUpdater()