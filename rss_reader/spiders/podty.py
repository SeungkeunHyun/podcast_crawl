import scrapy
from elasticsearch import Elasticsearch
import math
import time
import json
import os
""" ('177643', '8OPIJWcBgwbcB8rTPFA5'),
             ('174141', 'O-PJJWcBgwbcB8rTZVFv'), """


class PodtySpider(scrapy.Spider):
    name = 'podty'
    es = Elasticsearch(["localhost:9200"])

    def getCastsByProvider(self, provider):
        casts = self.es.search(index='casts', body={"from": 0, "size": 100, "query": {"term": {
                               "provider.keyword": provider}}},  filter_path=['hits.hits._id', 'hits.hits._source.feedURL'])
        casts = casts['hits']['hits']
        self.log(casts)
        for cast in casts:
            self.log(cast)
            cast['feedURL'] = cast['_source']['feedURL']
            cast.pop('_source')
        self.log(casts)
        return casts

    def start_requests(self):
        self.casts = self.getCastsByProvider('podty')
        self.log(self.casts)
        for cast in self.casts:
            self.log(cast)
            purl = cast['feedURL'] + "/episodes?page=1&dir=desc"
            yield scrapy.Request(url=purl, callback=self.parse)

    def parse(self, response):
        urlParts = response.url.split('/')
        hasMore = True
        esKey = urlParts[4]
        self.log("current parent id " + esKey)
        epCount = int(response.xpath(
            '//*[@id="container"]/div/nav/ul/li[1]/a/span/text()').extract_first())
        cpage = int(response.url.split("?")[1].split("&")[0].split("=")[1])
        tpage = math.ceil(float(epCount)/15)
        epList = response.css("ul.episodeList")[0]
        for episode in epList.css('li'):
            item = {}
            item['title'] = episode.xpath(
                './@data-episode-name').extract_first()
            item['pubDate'] = episode.xpath(
                './div/time[@class="date"]/text()').extract_first().replace('.', '/').strip()
            """ item['subtitle'] = episode.xpath(
                './itunes:subtitle').xpath('./text()').extract_first()
            item['summary'] = episode.xpath(
                './itunes:summary').xpath('./text()').extract_first() """
            item['mediaURL'] = episode.xpath(
                './@data-play-uri').extract_first()
            item['duration'] = episode.xpath(
                './div/time[@class="playTime"]/text()').extract_first().strip()
            item['cast_episode'] = {"name": "episode", "parent": esKey}
            hasMore = self.postToES(item, esKey)
            if not hasMore:
                break

        if hasMore and len(response.xpath("//a[@class='btnNext']")) > 0:
            yield scrapy.Request(url=response.url.split("?")[0] + "?page=" + str(cpage + 1) + "&dir=desc", callback=self.parse)

    def postToES(self, episode, parentId):
        if parentId == '174141':
            self.log(episode)
        res = self.es.count(index="casts", body={"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}})
        if dict(res)['count'] == 0:
            res = self.es.index(index='casts', routing=parentId,
                                doc_type='_doc', body=episode)
            return True
        else:
            return False
