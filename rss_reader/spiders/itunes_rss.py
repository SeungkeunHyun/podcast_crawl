# -*- coding: utf-8 -*-
import scrapy
from elasticsearch import Elasticsearch
from dateparser import parse
import json
import re
from pprint import pprint


class iTUnesSpider(scrapy.Spider):
    name = "itunes"
    es = Elasticsearch(["localhost:9200"])
    casts = {}

    def getCastsByProvider(self, provider):
        results = self.es.search(index='casts', body={"size": 100, "query": {"term": {
            "provider.keyword": provider}}})
        results = results['hits']['hits']
        for result in results:
            cast = result['_source']
            self.casts[cast['feedURL']] = cast
        self.log(self.casts)

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
        self.getCastsByProvider('itunes')
        self.log("print casts")
        for feedURL, cast in self.casts.items():
            self.log(cast)
            yield scrapy.Request(url=feedURL, callback=self.parse)

    def getkey(self, ep):
        # print(ep.xpath('./pubDate/text()').extract_first())
        dt = ep.xpath('./pubDate/text()').extract_first()
        dt = dt.replace(' :', ':')
        dt = dt.split(' +')[0]
        return parse(dt)

    def parse(self, response):
        cast = self.casts[response.url]
        esKey = cast['podcastID']
        # self.log(response.body.decode('utf-8'))
        namespaces = response.selector.re(r'xmlns\:(\S+)')
        for ns in namespaces:
            ns_def = ns.split("=")
            response.selector.register_namespace(
                ns_def[0], ns_def[1].replace('"', ''))
            self.log(ns_def[0] + '=' + ns_def[1].replace('"', ''))
        imgURL = response.xpath(
            '/rss/channel/itunes:image/@href').extract_first()
        if 'imageURL' not in cast or cast['imageURL'] != imgURL:
            cast['imageURL'] = imgURL
            cast['name'] = response.xpath(
                '/rss/channel/title/text()').extract_first()
            self.log('found to be updated %s' % imgURL)
            self.es.index(index='casts', doc_type='_doc',
                          id=cast['podcastID'], body=cast)
        episodes = response.xpath('/rss/channel/item')
        episodes = sorted(episodes, key=self.getkey, reverse=True)
        for episode in episodes:
            item = {}
            item['title'] = episode.xpath('./title/text()').extract_first()
            item['pubDate'] = self.getkey(
                episode).strftime('%Y/%m/%d %H:%M:%S')
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
