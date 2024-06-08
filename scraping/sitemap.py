import time
from pathlib import Path

import scrapy
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from scrapy import Item
from scrapy.spiders import SitemapSpider


class Sitemap(SitemapSpider):
    name = 'SiteSpider'

    sitemap_urls = ['https://www.juniper.net/documentation/sitemap/sitemap.xml']

    def parse(self, response, **kwargs):
        print('parse_article url:', response.url)
        with open('body.txt', 'w') as f:
            f.write(response.text)

        # yield {'text': response.url}


