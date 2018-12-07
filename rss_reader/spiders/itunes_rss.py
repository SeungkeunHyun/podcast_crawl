# -*- coding: utf-8 -*-
import scrapy
from elasticsearch import Elasticsearch
from dateutil.parser import parse
import json
from pprint import pprint


class iTUnesSpider(scrapy.Spider):
    name = "itunes"
    es = Elasticsearch(["localhost:9200"])
    esDic = es.search
    casts = None

    def getCastsByProvider(self, provider):
        casts = self.es.search(index='casts', body={"size": 100, "query": {"term": {
                               "provider.keyword": provider}}},  filter_path=['hits.hits._id', 'hits.hits._source.feedURL'])
        casts = casts['hits']['hits']
        for cast in casts:
            cast['feedURL'] = cast['_source']['feedURL']
            cast.pop('_source')
        self.log(casts)
        return casts

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.count(index="casts", body=json.dumps({"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}}))
        if dict(res)['count'] == 0:
            res = self.es.index(index='casts',
                                routing=parentId,
                                doc_type='_doc', body=episode)
            self.log(res)
            return True
        else:
            return False

    def start_requests(self):
        self.casts = self.getCastsByProvider('itunes')
        self.log("print casts")
        for cast in self.casts:
            self.log(cast)
            self.log(cast['feedURL'])
            yield scrapy.Request(url=cast['feedURL'], callback=self.parse)

    def parse(self, response):
        esKey = None
        for cast in self.casts:
            if cast['feedURL'] == response.url:
                esKey = cast['_id']
                break
        # self.log(response.body.decode('utf-8'))
        namespaces = response.selector.re(r'xmlns\:(\S+)')
        for ns in namespaces:
            ns_def = ns.split("=")
            response.selector.register_namespace(
                ns_def[0], ns_def[1].replace('"', ''))
            self.log(ns_def[0] + '=' + ns_def[1].replace('"', ''))
        for episode in response.xpath('/rss/channel/item'):
            item = {}
            item['title'] = episode.xpath('./title/text()').extract_first()
            item['pubDate'] = parse(episode.xpath(
                './pubDate/text()').extract_first()).strftime('%Y/%m/%d %H:%M:%S')
            item['subtitle'] = episode.xpath(
                './itunes:subtitle').xpath('./text()').extract_first()
            item['summary'] = episode.xpath(
                './itunes:summary').xpath('./text()').extract_first()
            item['mediaURL'] = episode.xpath(
                './enclosure/@url').extract_first()
            item['duration'] = episode.xpath(
                './itunes:duration').xpath('./text()').extract_first()
            item['cast_episode'] = {"name": "episode", "parent": esKey}
            if not self.postToES(item, esKey):
                break
