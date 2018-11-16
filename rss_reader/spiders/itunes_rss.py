# -*- coding: utf-8 -*-
import scrapy


class iTUnesSpider(scrapy.Spider):
    name = "idwk"

    def start_requests(self):
        urls = [
            'http://www.xsfm.co.kr/xml/idwk.xml'
        ]

        for url in urls:
            self.log(url)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
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
            item['title'] = episode.xpath('./title/text()').extract_first()
            item['pubDate'] = episode.xpath(
                './pubDate/text()').extract_first()
            item['subtitle'] = episode.xpath(
                './itunes:subtitle').xpath('./text()').extract_first()
            item['summary'] = episode.xpath(
                './itunes:summary').xpath('./text()').extract_first()
            item['mediaURL'] = episode.xpath(
                './enclosure/@url').extract_first()
            item['duration'] = episode.xpath(
                './itunes:duration').xpath('./text()').extract_first()
            episodes.append(item)
        self.log(episodes)
