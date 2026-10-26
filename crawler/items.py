import scrapy

class ProductDetailItem(scrapy.Item):
    # Metadata fields
    list_id = scrapy.Field()
    url = scrapy.Field()
    crawled_at = scrapy.Field()

    # Core content fields
    price = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()

    # Hardware specs (human-readable / extracted)
    cpu = scrapy.Field()
    ram = scrapy.Field()
    hard_drive = scrapy.Field()
    hard_drive_type = scrapy.Field()

    # Seller metrics
    seller_rating = scrapy.Field()
    seller_sold_count = scrapy.Field()

    # Category codes & location
    pc_brand = scrapy.Field()
    pc_cpu = scrapy.Field()
    pc_ram = scrapy.Field()
    region_name = scrapy.Field()
    ward_name = scrapy.Field()
    elt_condition = scrapy.Field()
    elt_warranty = scrapy.Field()
    elt_origin = scrapy.Field()
    laptop_screen_size = scrapy.Field()