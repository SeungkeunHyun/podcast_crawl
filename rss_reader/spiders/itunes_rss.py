# -*- coding: utf-8 -*-
import scrapy
from elasticsearch import Elasticsearch
from dateutil.parser import parse
""" 
('http://wizard2.sbs.co.kr/w3/podcast/V2000010143.xml', 's_FtC2cBD-yCrH-UjUCu'),
("http://feeds.feedburner.com/KoreanPodcastScienceAndMedicine",
 "zvFtC2cBD-yCrH-UjUCx"),
('http://minicast.imbc.com/PodCast/pod.aspx?code=1000671100000100000',
 'vvFtC2cBD-yCrH-UjUCv'),
('http://www.xsfm.co.kr/xml/idwk.xml', 'yvFtC2cBD-yCrH-UjUCx'),
('http://feeds.feedburner.com/KIMYoungha', 'lPFtC2cBD-yCrH-UjUCs'),
('http://old.ddanzi.com/appstream/bunker1season2.xml',
 '1_FtC2cBD-yCrH-UjkB3') """


class iTUnesSpider(scrapy.Spider):
    name = "itunes"
    es = Elasticsearch(["localhost:9200"])
    esDic = [('http://wizard2.sbs.co.kr/w3/podcast/V2000010143.xml', 's_FtC2cBD-yCrH-UjUCu'),
             ("http://feeds.feedburner.com/KoreanPodcastScienceAndMedicine",
              "zvFtC2cBD-yCrH-UjUCx"),
             ('http://minicast.imbc.com/PodCast/pod.aspx?code=1000671100000100000',
              'vvFtC2cBD-yCrH-UjUCv'),
             ('http://www.xsfm.co.kr/xml/idwk.xml', 'yvFtC2cBD-yCrH-UjUCx'),
             ('http://feeds.feedburner.com/KIMYoungha', 'lPFtC2cBD-yCrH-UjUCs'),
             ('http://old.ddanzi.com/appstream/bunker1season2.xml',
              '1_FtC2cBD-yCrH-UjkB3')
             ]

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.index(index='cast_episodes',
                            parent=parentId,
                            doc_type='_doc', body=episode)
        self.log(res)

    def start_requests(self):
        for url, v in self.esDic:
            self.log(url)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        esKey = dict(self.esDic)[response.url]
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
            self.postToES(item, esKey)
