from unittest.mock import MagicMock
from crawler.pipeline import MongoPipeline
from crawler.items import ProductDetailItem


def test_mongo_pipeline_process_item_with_list_id():
    """Kiểm tra pipeline ưu tiên query theo list_id để upsert khi có list_id."""
    pipeline = MongoPipeline("mongodb://localhost:27017", "test_db")
    pipeline.db = MagicMock()
    mock_collection = MagicMock()
    pipeline.db.__getitem__.return_value = mock_collection

    item = ProductDetailItem()
    item["list_id"] = 12345
    item["url"] = "https://www.chotot.com/12345.htm"
    item["title"] = "Laptop test"

    spider_mock = MagicMock()
    result = pipeline.process_item(item, spider_mock)

    assert result == item
    mock_collection.update_one.assert_called_once()
    args, kwargs = mock_collection.update_one.call_args
    assert args[0] == {"list_id": 12345}
    assert kwargs.get("upsert") is True


def test_mongo_pipeline_process_item_fallback_url():
    """Kiểm tra pipeline fallback theo url nếu bài đăng không có list_id."""
    pipeline = MongoPipeline("mongodb://localhost:27017", "test_db")
    pipeline.db = MagicMock()
    mock_collection = MagicMock()
    pipeline.db.__getitem__.return_value = mock_collection

    item = ProductDetailItem()
    item["url"] = "https://www.chotot.com/legacy_laptop.htm"
    item["title"] = "Laptop cu"

    spider_mock = MagicMock()
    result = pipeline.process_item(item, spider_mock)

    assert result == item
    mock_collection.update_one.assert_called_once()
    args, kwargs = mock_collection.update_one.call_args
    assert args[0] == {"url": "https://www.chotot.com/legacy_laptop.htm"}
    assert kwargs.get("upsert") is True
