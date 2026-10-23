import pymongo
from itemadapter import ItemAdapter

class MongoPipeline:
    collection_name = "Laptop_Crawler"
    
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "scrapy_db")
        )
        
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        
    def close_spider(self, spider):
        self.client.close()
        
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        data = adapter.asdict()
        
        self.db[self.collection_name].update_one(
            {"url": data["url"]},
            {"$set": data},
            upsert=True
        )
        return item