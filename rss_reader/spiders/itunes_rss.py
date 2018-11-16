# -*- coding: utf-8 -*-
import scrapy
from elasticsearch import Elasticsearch
from dateutil.parser import parse


class iTUnesSpider(scrapy.Spider):
    name = "itunes"
    es = Elasticsearch(["localhost:9200"])
    esDic = [('http://www.xsfm.co.kr/xml/idwk.xml', 'yvFtC2cBD-yCrH-UjUCx')]

    def postToES(self, episode):
        self.log(episode)
        res = self.es.index(index='cast_episodes',
                            doc_type='_doc', body=episode)
        self.log(res)

    def start_requests(self):
        for url, v in self.esDic:
            self.log(url)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        esKey = dict(self.esDic)[response.url]
        episodes = list()
        namespaces = response.selector.re(r'xmlns\:(\S+)')
        for ns in namespaces:
            ns_def = ns.split("=")
            response.selector.register_namespace(
                ns_def[0], ns_def[1].replace('"', ''))
            self.log(ns_def[0] + '=' + ns_def[1].replace('"', ''))
        self.log(response)
        for episode in response.xpath('/rss/channel/item'):
            item = {}
            item['parentId'] = esKey
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
            episodes.append(item)
            self.log(item)
            self.postToES(item)
