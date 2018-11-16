import scrapy


class PodtySpider(scrapy.Spider):
    name = 'podty'

    def start_requests(self):
        casts = ['174141', '177643']
        for castId in casts:
            yield scrapy.Request(url='https://www.podty.me/cast/' + castId, callback=self.parse)

    def parse(self, response):
        episodes = list()
        epList = response.css("ul.episodeList")[0]
        for episode in epList.css('li'):
            item = {}
            item['title'] = episode.xpath(
                './@data-episode-name').extract_first()
            item['pubDate'] = episode.xpath(
                './div/time[@class="date"]/text()').extract_first().replace('.', '/')
            """ item['subtitle'] = episode.xpath(
                './itunes:subtitle').xpath('./text()').extract_first()
            item['summary'] = episode.xpath(
                './itunes:summary').xpath('./text()').extract_first() """
            item['mediaURL'] = episode.xpath(
                './@data-play-uri').extract_first()
            item['duration'] = episode.xpath(
                './div/time[@class="playTime"]/text()').extract_first()
            episodes.append(item)

        self.log(episodes)
