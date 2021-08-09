import scrapy
import re
import json
import dateparser
from datetime import datetime
from elasticsearch import Elasticsearch
import ast
import hashlib
""" ('7339', 'XuPJJWcBgwbcB8rTZVFz'),
('10135', '8ePIJWcBgwbcB8rTPFA5'), """


class PodbbangSpider(scrapy.Spider):
    name = "podbbang"
    es = Elasticsearch(["localhost:9200"])
    casts = []

    def getCastsByProvider(self, provider):
        results = self.es.search(index='casts', body={"size": 500, "query": {"term": {
            "provider.keyword": provider}}})
        results = results['hits']['hits']
        for result in results:
            self.casts.append(result['_source'])
        #self.log(self.casts)

    def start_requests(self):
        self.getCastsByProvider(self.name)
        self.log('test %d' % len(self.casts))
        for cast in self.casts:
            self.log(cast)
            purl = "https://app-api6.podbbang.com/channels/%s/episodes?offset=0&sort=desc&limit=30&episode_id=0" % cast['podcastID']
            self.log("feed url: %s" % purl)
            yield scrapy.Request(url=purl, callback=self.parse)

    def parse(self, response):
        castID = re.search('/(\d+)/', response.url).group(1)
        episodes = json.loads(response.body)
        esEP = {}
        for ep in episodes['data']:
            self.log(ep)
            esEP['title'] = ep['title']
            esEP['pubDate'] = dateparser.parse(ep['publishedAt']).isoformat()
            esEP['duration'] = ep['media']['duration']
            esEP['mediaURL'] = ep['media']['url']
            esEP['description'] = ep['description']
            esEP['cast_episode'] = castID
            esEP['id'] = hashlib.md5(esEP['mediaURL'].encode()).hexdigest()
            self.postToES(esEP)

    def postToES(self, episode):
        qry = json.dumps({"query": {"term": {"mediaURL.keyword": episode['mediaURL']}}})
        self.log(episode)
        self.log(qry)
        res = self.es.count(index="episodes", body=qry)
        if dict(res)['count'] == 0:
            res = self.es.index(index='episodes', id=episode['id'],
                                doc_type='_doc', body=episode)
            self.log(res)
            return True
        else:
            return False
