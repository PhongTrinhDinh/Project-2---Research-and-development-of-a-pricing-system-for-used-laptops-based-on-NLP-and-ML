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
        
    def open_spider(self, spider=None):
        if not self.mongo_uri:
            self.client = None
            self.db = None
            return
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        
    def close_spider(self, spider=None):
        if self.client:
            self.client.close()
        
    def process_item(self, item, spider=None):
        if self.db is None:
            return item

        adapter = ItemAdapter(item)
        data = adapter.asdict()
        
        query = {"list_id": data["list_id"]} if data.get("list_id") else {"url": data.get("url")}
        self.db[self.collection_name].update_one(
            query,
            {"$set": data},
            upsert=True
        )
        return item
