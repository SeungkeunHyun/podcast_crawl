import scrapy
from elasticsearch import Elasticsearch
import math
import datetime
import json
import os
import dateparser
import re
import hashlib
import lxml.html 

""" ('177643', '8OPIJWcBgwbcB8rTPFA5'),
             ('174141', 'O-PJJWcBgwbcB8rTZVFv'), """


class PodtySpider(scrapy.Spider):
    name = 'podty'
    es = Elasticsearch(["localhost:9200"])
    casts = {}

    def getCastsByProvider(self, provider):
        results = self.es.search(index='casts', body={"from": 0, "size": 100, "query": {"term": {
            "provider.keyword": provider}}})
        results = results['hits']['hits']
        for result in results:
            self.casts[result['_id']] = result['_source']

    def start_requests(self):
        self.getCastsByProvider('podty')
        self.log(self.casts)
        for esKey, cast in self.casts.items():
            self.log(cast)
            purl = "https://www.podty.me/cast/%s/episode/list?orderType=desc" % esKey
            self.log(purl)
            yield scrapy.Request(url=purl, callback=self.parse)

    def parse(self, response):
        esKey = re.search('\/\d+\/', response.url).group(0).replace('/','')
        self.log(response.url)
        self.log(esKey)
        rbody = response.json()
        hasMore = True
        #self.log(rbody['list_html'])
        htmlEp = lxml.html.fromstring(rbody['list_html'])
        #cast = self.casts[esKey]
        #self.log(cast)
        # imgURL = response.selector.xpath(
        #     '//*[@id="container"]/section/div[1]/img/@src').extract_first()
        # self.log(imgURL)
        # if 'imageURL' not in cast or cast['imageURL'] != imgURL or 'author' not in cast:
        #     cast['author'] = response.selector.xpath(
        #         '//*[@id="container"]/section/div/ul[@class="subInfo"]/li[1]/strong/text()').extract_first()
        #     cast['imageURL'] = imgURL
        #     cast['name'] = response.selector.xpath(
        #         '//*[@id="container"]/section/div[2]/div[1]/p/text()').extract_first()
        #     self.es.index(index='casts', id=esKey, doc_type='_doc', body=cast)
        # self.log("current parent id " + esKey)
        for episode in htmlEp.cssselect('li.episodeItem'):
            self.log(type(episode))
            item = {}
            item['title'] = episode.xpath('@data-episode-name')[0]
            self.log(episode.xpath('.//div[@class="episodeInfo"]/time[@class="date"]/text()')[0])
            item['pubDate'] = episode.xpath('.//div[@class="episodeInfo"]/time[@class="date"]/text()')[0].replace('.', '/').strip()
            if item['pubDate'] == '1일 전':
                item['pubDate'] = (datetime.datetime.now() + datetime.timedelta(days=-1))
            else:
                item['pubDate'] = dateparser.parse(item['pubDate'])
            item['pubDate'] = datetime.datetime.isoformat(item['pubDate'])
            item['mediaURL'] = episode.xpath('@data-play-uri')[0]
            self.log(item)
            item['duration'] = episode.xpath('.//div[@class="playTime"]/time/text()')[0].strip()
            item['cast_episode'] = esKey
            item['id'] = hashlib.md5(item['mediaURL'].encode()).hexdigest()
            hasMore = self.postToES(item)
            if not hasMore:
                break
        self.updateCast(esKey)

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


    def postToES(self, episode):
        self.log(episode)
        res = self.es.count(index="episodes", body={"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}})
        if dict(res)['count'] == 0:
            res = self.es.index(index='episodes', id=episode['id'],
                                doc_type='_doc', body=episode)
            return True
        else:
            return False
