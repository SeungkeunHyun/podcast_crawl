# -*- coding: utf-8 -*-
import scrapy
from elasticsearch import Elasticsearch
from dateutil.parser import parse
import json

"""     esDic = [('http://wizard2.sbs.co.kr/w3/podcast/V2000010143.xml', '_OPIJWcBgwbcB8rTPFA5'),
             ("http://feeds.feedburner.com/KoreanPodcastScienceAndMedicine",
              "F-PIJWcBgwbcB8rTPFFU"),
             ('http://minicast.imbc.com/PodCast/pod.aspx?code=1000671100000100000',
              '7uPIJWcBgwbcB8rTPFA2'),
             ('http://www.xsfm.co.kr/xml/idwk.xml', '8-PIJWcBgwbcB8rTPFA5'),
             ('http://feeds.feedburner.com/KIMYoungha', 'EuPIJWcBgwbcB8rTPFFM'),
             ('http://old.ddanzi.com/appstream/bunker1season2.xml',
              'MePJJWcBgwbcB8rTZVFt')
             ] """


""" ('http://api.podty.me/api/v1/share/cast/390937d3e5c758aa6f4005b63542cc83695b4d5e6925fe6a2d4d488d1d05d748/146364', '9uPIJWcBgwbcB8rTPFA5'),
             ('http://pod.ssenhosting.com/rss/geesik02/dokugawa.rss',
              'V-PJJWcBgwbcB8rTZVFy'),
             ('http://feeds.soundcloud.com/users/soundcloud:users:318037338/sounds.rss',
              '2-PIJWcBgwbcB8rTPFA0'),
             ('http://feeds.soundcloud.com/users/soundcloud:users:294637947/sounds.rss',
              'HePIJWcBgwbcB8rTPFFZ'),
             ('http://enabler.kbs.co.kr/api/podcast_channel/feed.xml?channel_id=R2018-0053',
              '5-PIJWcBgwbcB8rTPFA2'),
             ('http://svc.jtbc.joins.com/api/news/data/rss/JTBC_JtbcNews9_AOD_Podcast.xml',
              'MOPJJWcBgwbcB8rTZVFt'),
             ('http://rss.ohmynews.com/rss/podcast_EC_online_main.xml',
              '_-PIJWcBgwbcB8rTPFBA'),
             ('http://xsfm.co.kr/xml/podera.xml', 'PuPJJWcBgwbcB8rTZVFv'),
             ('http://xsfm.co.kr/xml/podera_GH.xml', 'FuPIJWcBgwbcB8rTPFFU'),
             ('http://wizard2.sbs.co.kr/w3/podcast/V0000365040.xml',
              'X-PJJWcBgwbcB8rTZVFz'),
             ('http://old.ddanzi.com/appstream/bunker1season2.xml',
              'MePJJWcBgwbcB8rTZVFt'),
             ('http://feeds.feedburner.com/KoreanPodcastSocietyAndCulture',
              'SuPJJWcBgwbcB8rTZVFy'),
             ('http://feeds.feedburner.com/KoreanPodcastHealth', 'AOPIJWcBgwbcB8rTPFFB'),
             ('http://feeds.feedburner.com/KoreanPodcastKidsAndFamily',
              '6uPIJWcBgwbcB8rTPFA2'),
             ('http://feeds.feedburner.com/KoreanPodcastScienceAndMedicine',
              'F-PIJWcBgwbcB8rTPFFU'),
             ('http://old.ddanzi.com/appstream/ttt.xml', 'YOPJJWcBgwbcB8rTZVFz'),
             ('http://api.podty.me/api/v1/share/cast/b4556578e4f143d676c7acb660ca358bd58568a192855be0988d133a6bfceaaa/174690', '3uPIJWcBgwbcB8rTPFA0'),
             ('http://old.ddanzi.com/appstream/highfidelity.xml',
              'CePIJWcBgwbcB8rTPFFJ'),
             ('http://podcast2.synapsetech.co.kr/users/badcoin/badcoin.xml',
              'E-PIJWcBgwbcB8rTPFFM'),
             ('http://old.ddanzi.com/appstream/possible_dog.xml',
              'TOPJJWcBgwbcB8rTZVFy'), 
             ('https://podcasts.files.bbci.co.uk/p02pc9tn.rss', 'AuPIJWcBgwbcB8rTPFFB'),
             ('http://xsfm.co.kr/xml/p25.xml', '7OPIJWcBgwbcB8rTPFA2'),
             ('http://podcast.nexusbook.com/xml_source/midrealconv.xml',
              'U-PJJWcBgwbcB8rTZVFy'),
             ('http://svc.jtbc.joins.com/api/Tv/data/rss/JTBC_ssulzun_AOD_Podcast.xml',
              'C-PIJWcBgwbcB8rTPFFJ') """


class iTUnesSpider(scrapy.Spider):
    name = "itunes"
    es = Elasticsearch(["localhost:9200"])
    esDic = [('http://api.podty.me/api/v1/share/cast/390937d3e5c758aa6f4005b63542cc83695b4d5e6925fe6a2d4d488d1d05d748/146364', '9uPIJWcBgwbcB8rTPFA5'),
             ('http://pod.ssenhosting.com/rss/geesik02/dokugawa.rss',
              'V-PJJWcBgwbcB8rTZVFy'),
             ('http://feeds.soundcloud.com/users/soundcloud:users:318037338/sounds.rss',
              '2-PIJWcBgwbcB8rTPFA0'),
             ('http://feeds.soundcloud.com/users/soundcloud:users:294637947/sounds.rss',
              'HePIJWcBgwbcB8rTPFFZ'),
             ('http://enabler.kbs.co.kr/api/podcast_channel/feed.xml?channel_id=R2018-0053',
              '5-PIJWcBgwbcB8rTPFA2'),
             ('http://svc.jtbc.joins.com/api/news/data/rss/JTBC_JtbcNews9_AOD_Podcast.xml',
              'MOPJJWcBgwbcB8rTZVFt'),
             ('http://rss.ohmynews.com/rss/podcast_EC_online_main.xml',
              '_-PIJWcBgwbcB8rTPFBA'),
             ('http://xsfm.co.kr/xml/podera.xml', 'PuPJJWcBgwbcB8rTZVFv'),
             ('http://xsfm.co.kr/xml/podera_GH.xml', 'FuPIJWcBgwbcB8rTPFFU'),
             ('http://wizard2.sbs.co.kr/w3/podcast/V0000365040.xml',
              'X-PJJWcBgwbcB8rTZVFz'),
             ('http://old.ddanzi.com/appstream/bunker1season2.xml',
              'MePJJWcBgwbcB8rTZVFt'),
             ('http://feeds.feedburner.com/KoreanPodcastSocietyAndCulture',
              'SuPJJWcBgwbcB8rTZVFy'),
             ('http://feeds.feedburner.com/KoreanPodcastHealth', 'AOPIJWcBgwbcB8rTPFFB'),
             ('http://feeds.feedburner.com/KoreanPodcastKidsAndFamily',
              '6uPIJWcBgwbcB8rTPFA2'),
             ('http://feeds.feedburner.com/KoreanPodcastScienceAndMedicine',
              'F-PIJWcBgwbcB8rTPFFU'),
             ('http://old.ddanzi.com/appstream/ttt.xml', 'YOPJJWcBgwbcB8rTZVFz'),
             ('http://api.podty.me/api/v1/share/cast/b4556578e4f143d676c7acb660ca358bd58568a192855be0988d133a6bfceaaa/174690', '3uPIJWcBgwbcB8rTPFA0'),
             ('http://old.ddanzi.com/appstream/highfidelity.xml',
              'CePIJWcBgwbcB8rTPFFJ'),
             ('http://podcast2.synapsetech.co.kr/users/badcoin/badcoin.xml',
              'E-PIJWcBgwbcB8rTPFFM'),
             ('http://old.ddanzi.com/appstream/possible_dog.xml',
              'TOPJJWcBgwbcB8rTZVFy'),
             ('https://podcasts.files.bbci.co.uk/p02pc9tn.rss', 'AuPIJWcBgwbcB8rTPFFB'),
             ('http://xsfm.co.kr/xml/p25.xml', '7OPIJWcBgwbcB8rTPFA2'),
             ('http://podcast.nexusbook.com/xml_source/midrealconv.xml',
              'U-PJJWcBgwbcB8rTZVFy'),
             ('http://svc.jtbc.joins.com/api/Tv/data/rss/JTBC_ssulzun_AOD_Podcast.xml',
              'C-PIJWcBgwbcB8rTPFFJ')]

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.count(index="casts", body=json.dumps({"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}}))
        if dict(res)['count'] == 0:
            res = self.es.index(index='casts',
                                routing=parentId,
                                doc_type='doc', body=episode)
            self.log(res)

    def start_requests(self):
        for url, v in self.esDic:
            self.log(url)
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        esKey = dict(self.esDic)[response.url]
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
            item['join_field'] = {"name": "episode", "parent": esKey}
            self.postToES(item, esKey)
