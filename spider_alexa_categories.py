import scrapy
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors import LinkExtractor
from alexa.items import AlexaItem
from scrapy.selector import HtmlXPathSelector

class MySpider(CrawlSpider):
    name = 'spider_alexa'
    allowed_domains = ['alexa.com']
    start_urls = ['http://www.alexa.com/topsites']
                   #'http://www.alexa.com/topsites/category/Top/',
                   #'http://www.alexa.com/topsites/category/Top/News']
                   #'http://www.alexa.com/topsites/category/Top/Regional']
                   #'http://www.alexa.com/topsites/category/Top/Arts']
                   #'http://www.alexa.com/topsites/category/Top/Business']
                   #'http://www.alexa.com/topsites/category/Top/Adult']
                   #'http://www.alexa.com/topsites/category/Top/Health']
                   #'http://www.alexa.com/topsites/category/Top/Home']
                   #'http://www.alexa.com/topsites/category/Top/Kids_and_Teens']
                   #'http://www.alexa.com/topsites/category/Top/Recreation']
                   #'http://www.alexa.com/topsites/category/Top/Reference']
                   #'http://www.alexa.com/topsites/category/Top/Science']
                   #'http://www.alexa.com/topsites/category/Top/Shopping']
                   #'http://www.alexa.com/topsites/category/Top/Society']
                   #'http://www.alexa.com/topsites/category/Top/Sports']
                   #'http://www.alexa.com/topsites/category/Top/World']

    rules = (
        # Extract links matching '/topsites/global;\d'
        # and follow links from them (since no callback means follow=True by default).
        Rule(LinkExtractor(allow=('/topsites/global;\d', ), restrict_xpaths=('//a[@class="next"]', )), callback='parse_item',follow= True),
            Rule(LinkExtractor(allow=('/topsites/category;\d', ), restrict_xpaths=('//a[@class="next"]', )), callback='parse_item',follow= True),
    )
    
    # to extract from first page
    def parse_start_url(self, response):
        return self.parse_item(response)

    def parse_item(self, response):
        self.log('Hi, this is an item page! %s' % response.url)
        item = AlexaItem()
        item['Name'] = response.xpath('//div[@class="desc-container"]/p[@class="desc-paragraph"]/a/text()').extract()
        return item
    
        
