from lxml import html
import xml.etree.ElementTree as xml
import requests
from pprint import pprint

page = requests.get("http://xsfm.co.kr/xml/srsr.xml")
xdom = html.fromstring(page.content)
pprint(xdom.nsmap)
items = xdom.xpath("//item")
for item in items:
    pprint(item.find('pubDate'))
