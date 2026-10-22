import scrapy
import os

class ProductDetailItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    price = scrapy.Field()
    location = scrapy.Field()
    description = scrapy.Field()
    crawled_at = scrapy.Field()
    brand = scrapy.Field()
    line_product = scrapy.Field()
    device_condition = scrapy.Field()
    screen_size = scrapy.Field()
    cpu = scrapy.Field()
    ram = scrapy.Field()
    graphic_card = scrapy.Field()
    hard_drive = scrapy.Field()
    hard_drive_type = scrapy.Field()
    
ITEM_PIPELINES = {
    "crawler.pipeline.MongoPipeline": 300,
}

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DATABASE = "choto_db"