import scrapy
import re
import json
import datetime
from elasticsearch import Elasticsearch

""" ('7339', 'XuPJJWcBgwbcB8rTZVFz'),
('10135', '8ePIJWcBgwbcB8rTPFA5'), """


class PodbbangSpider(scrapy.Spider):
    name = "podbbang"
    es = Elasticsearch(["localhost:9200"])
    esDic = [('12548', 'KuPJJWcBgwbcB8rTZVFt'),
             ('10240', '9ePIJWcBgwbcB8rTPFA5'),
             ('11168', 'R-PJJWcBgwbcB8rTZVFy'),
             ('7453', '4uPIJWcBgwbcB8rTPFA2'),
             ('8199', 'OOPJJWcBgwbcB8rTZVFv'),
             ('10887', 'OePJJWcBgwbcB8rTZVFv'),
             ('12281', 'L-PJJWcBgwbcB8rTZVFt'),
             ('14285', 'IOPIJWcBgwbcB8rTPFFZ'),
             ('8336', 'WOPJJWcBgwbcB8rTZVFy'),
             ('5579', '3OPIJWcBgwbcB8rTPFA0'),
             ('6644', 'IePIJWcBgwbcB8rTPFFZ'),
             ('6205', 'WePJJWcBgwbcB8rTZVFy'),
             ('6643', '3ePIJWcBgwbcB8rTPFA0'),
             ('7290', 'I-PIJWcBgwbcB8rTPFFZ'),
             ('7193', 'AePIJWcBgwbcB8rTPFFB'),
             ('4301', '6-PIJWcBgwbcB8rTPFA2'),
             ('10306', 'UuPJJWcBgwbcB8rTZVFy'),
             ('8010', '3-PIJWcBgwbcB8rTPFA0'),
             ('12757', 'CuPIJWcBgwbcB8rTPFFJ')]

    def start_requests(self):
        for k, v in self.esDic:
            purl = f"http://www.podbbang.com/ch/{k}?page=1"
            self.log(purl)
            yield scrapy.Request(url=purl, callback=self.parse)

    def parse(self, response):
        esKey = dict(self.esDic)[response.url.split("/")[4].split("?")[0]]
        jsonItems = re.findall(
            'episode\[\d+\] = ({[^}]+})', response.body.decode('utf-8'))
        for jsonItem in jsonItems:
            episode = {}
            jsonItem += ""
            jsonItem = jsonItem.strip().replace('"', '`').replace(
                "'", '"').replace('"ischsell":ischsell', '"dummy": 1')
            self.log(jsonItem)
            datItem = json.loads(jsonItem)
            episode['title'] = datItem['title']
            episode['join_field'] = {"name": "episode",
                                     "parent": esKey}
            if datItem['pubdate'] == 'Today':
                episode['pubdate'] = datetime.today().strftime('%Y/%m/%d')
            else:
                episode['pubDate'] = datItem['pubdate'][:4] + '/' + \
                    datItem['pubdate'][4:6] + '/' + datItem['pubdate'][6:8]
            episode['mediaURL'] = datItem['down_file']
            episode['duration'] = response.selector.xpath('//li[@epiuid="' + datItem['player_down'].split(
                '=')[-1] + '"]/dl/dd[@class="dd_time"]/text()').extract_first().strip()
            self.postToES(episode, esKey)

        if len(response.xpath('//img[@alt="다음"]')) > 0:
            urlParts = response.url.split("=")
            purl = urlParts[0] + "=" + str(int(urlParts[1]) + 1)
            yield scrapy.Request(url=purl, callback=self.parse)

    def postToES(self, episode, parentId):
        self.log(episode)
        res = self.es.count(index="casts", body=json.dumps({"query": {"term": {
            "mediaURL.keyword": episode['mediaURL']}}}))
        if dict(res)['count'] == 0:
            res = self.es.index(index='casts', routing=parentId,
                                doc_type='doc', body=episode)
            self.log(res)
