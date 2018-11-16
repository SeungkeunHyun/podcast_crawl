import scrapy
from elasticsearch import Elasticsearch
import math
import time
import json
import os
""" ('174141', "wvFtC2cBD-yCrH-UjUCw"), """


class PodtySpider(scrapy.Spider):
    name = 'podty'
    casts = [('177643', 'wPFtC2cBD-yCrH-UjUCw'),
             ('174141', 'wvFtC2cBD-yCrH-UjUCw')]
    es = Elasticsearch(["localhost:9200"])
    pages = 10
    hold = True

    def start_requests(self):
        i = 0
        for k, v in self.casts:
            while i < self.pages:
                self.log("Total pages " + str(self.pages) +
                         " (current page: " + str(i+1) + ")")
                purl = 'https://www.podty.me/cast/' + k + "/episodes?page=" + str(i+1) + "&dir=desc"
                yield scrapy.Request(url=purl, callback=self.parse)
                i += 1
                if self.hold:
                    time.sleep(5)

    def parse(self, response):
        urlParts = response.url.split('/')
        esKey = dict(self.casts)[urlParts[4]]
        self.log("current parent id " + esKey)
        if self.pages == 10:
            epCount = int(response.xpath(
                '//*[@id="container"]/div/nav/ul/li[1]/a/span/text()').extract_first())
            self.pages = math.ceil(float(epCount)/15)
            self.hold = False

        self.log("total page is " + str(self.pages) + ": " + response.url)
        epList = response.css("ul.episodeList")
        if len(epList) == 0:
            self.log(str(response.body))
            return
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
            self.postToES(item, esKey)

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.count(index="cast_episodes", body=json.dumps({"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}}))
        if dict(res)['count'] == 0:
            res = self.es.index(index='cast_episodes', parent=parentId,
                                doc_type='_doc', body=episode)
            self.log(res)
        else:
            self.log('Exists. skip!')
