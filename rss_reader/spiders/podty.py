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
    casts = [('177643', '8OPIJWcBgwbcB8rTPFA5'),
             ('174141', 'O-PJJWcBgwbcB8rTZVFv'), ('182400', '6OPIJWcBgwbcB8rTPFA2')]
    es = Elasticsearch(["localhost:9200"])

    def start_requests(self):
        for k, v in self.casts:
            purl = 'https://www.podty.me/cast/' + k + "/episodes?page=1&dir=desc"
            yield scrapy.Request(url=purl, callback=self.parse)

    def parse(self, response):
        urlParts = response.url.split('/')
        esKey = dict(self.casts)[urlParts[4]]
        self.log("current parent id " + esKey)
        epCount = int(response.xpath(
            '//*[@id="container"]/div/nav/ul/li[1]/a/span/text()').extract_first())
        cpage = int(response.url.split("?")[1].split("&")[0].split("=")[1])
        tpage = math.ceil(float(epCount)/15)
        epList = response.css("ul.episodeList")
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
            item['join_field'] = {"name": "episode", "parent": esKey}
            self.postToES(item, esKey)

        if tpage > cpage:
            yield scrapy.Request(url=response.url.split("?")[0] + "?page=" + str(cpage + 1) + "&dir=desc", callback=self.parse)

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.count(index="casts", body=json.dumps({"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}}))
        if dict(res)['count'] == 0:
            res = self.es.index(index='casts', routing=parentId,
                                doc_type='doc', body=episode)
            self.log(res)
        else:
            self.log('Exists. skip!')
