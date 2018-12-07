import scrapy
import re
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import ast

""" ('7339', 'XuPJJWcBgwbcB8rTZVFz'),
('10135', '8ePIJWcBgwbcB8rTPFA5'), """


class PodbbangSpider(scrapy.Spider):
    name = "podbbang"
    es = Elasticsearch(["localhost:9200"])
    casts = None

    def getCastsByProvider(self, provider):
        casts = self.es.search(index='casts', body={"size": 100, "query": {"term": {
                               "provider.keyword": provider}}},  filter_path=['hits.hits._id', 'hits.hits._source.feedURL'])
        casts = casts['hits']['hits']
        for cast in casts:
            cast['feedURL'] = cast['_source']['feedURL']
            cast.pop('_source')
        self.log(casts)
        return casts

    def start_requests(self):
        self.casts = self.getCastsByProvider('podbbang')
        for cast in self.casts:
            purl = cast['feedURL'] + "?page=1"
            self.log(purl)
            yield scrapy.Request(url=purl, callback=self.parse)

    def parse(self, response):
        esKey = response.url.split("/")[4].split("?")[0]
        hasMore = True
        jsonItems = re.findall(
            'episode\[\d+\] = ({[^}]+})', response.body.decode('utf-8'))
        for jsonItem in jsonItems:
            episode = {}
            jsonItem += ""
            jsonItem = jsonItem.strip().replace("'ischsell':ischsell", '"dummy": 1')
            # self.log(jsonItem)
            datItem = ast.literal_eval(jsonItem)
            episode['title'] = datItem['title']
            episode['cast_episode'] = {"name": "episode",
                                       "parent": esKey}
            if datItem['pubdate'] == 'Today':
                episode['pubdate'] = datetime.today().strftime('%Y/%m/%d')
            else:
                episode['pubDate'] = datItem['pubdate'][:4] + '/' + \
                    datItem['pubdate'][4:6] + '/' + datItem['pubdate'][6:8]
            episode['mediaURL'] = datItem['down_file']
            episode['duration'] = response.selector.xpath('//li[@epiuid="' + datItem['player_down'].split(
                '=')[-1] + '"]/dl/dd[@class="dd_time"]/text()').extract_first().strip()
            hasMore = self.postToES(episode, esKey)
            if not hasMore:
                break

        if hasMore and len(response.xpath('//img[@alt="다음"]')) > 0:
            urlParts = response.url.split("=")
            purl = urlParts[0] + "=" + str(int(urlParts[1]) + 1)
            yield scrapy.Request(url=purl, callback=self.parse)

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.count(index="casts", body=json.dumps({"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}}))
        if dict(res)['count'] == 0:
            res = self.es.index(index='casts', routing=parentId,
                                doc_type='_doc', body=episode)
            self.log(res)
            return True
        else:
            return False
