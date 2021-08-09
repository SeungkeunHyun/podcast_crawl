#!/usr/bin/bash
export PATH=$PATH:/home/sk/.local/bin
CDIR="$(dirname "$0")"
cd $CDIR
scrapy crawl itunes 
scrapy crawl podbbang 
scrapy crawl podty
$CDIR/cast_update.py
