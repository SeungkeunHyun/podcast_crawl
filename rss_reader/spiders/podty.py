import scrapy
from elasticsearch import Elasticsearch


class PodtySpider(scrapy.Spider):
    name = 'podty'
    casts = [('174141', "wPFtC2cBD-yCrH-UjUCw"),
             ('177643', 'wvFtC2cBD-yCrH-UjUCw')]
    es = Elasticsearch(["localhost:9200"])

    def start_requests(self):
        for k, v in self.casts:
            yield scrapy.Request(url='https://www.podty.me/cast/' + k, callback=self.parse)

    def parse(self, response):
        esKey = dict(self.casts)[response.url.split('/')[-1]]
        episodes = list()
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
            item['parentId'] = esKey
            episodes.append(item)
            # self.postToES(item)

    def postToES(self, episode):
        self.log(episode)
        res = self.es.index(index='cast_episodes',
                            doc_type='_doc', body=episode)
        self.log(res)
