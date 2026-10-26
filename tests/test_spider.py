import json
import pytest
from scrapy.http import TextResponse, Request
from crawler.spiders.master_detail_spider import MasterDetailSpider
from crawler.items import ProductDetailItem


@pytest.fixture
def spider():
    return MasterDetailSpider()


@pytest.fixture
def sample_ad():
    return {
        "list_id": 135459559,
        "price": 12800000,
        "subject": "  Macbook Pro CORE I9 - 9880H với SSD 1TB  ",
        "body": "  💥-----BÁN 12tr8 Macbook Pro 16 2019 MÁY CHÍNH CHỦ...  ",
        "average_rating": 4.5,
        "sold_ads": 250,
        "pc_brand": 3,
        "pc_cpu": 12,
        "pc_ram": 7,
        "pc_drive_capacity": 12,
        "pc_drive_type": 2,
        "region_name_v3": "TP Hồ Chí Minh",
        "ward_name_v3": "Phường Hạnh Thông",
        "elt_condition": 2,
        "elt_warranty": 6,
        "elt_origin": 8,
        "laptop_screen_size": 5,
        "params": [
            {"id": "pc_cpu", "label": "Bộ vi xử lý", "value": "Intel Core i9"},
            {"id": "pc_ram", "label": "RAM", "value": "16 GB"},
            {"id": "pc_drive_capacity", "label": "Ổ cứng", "value": "1 TB"},
            {"id": "pc_drive_type", "label": "Loại ổ cứng", "value": "SSD"},
        ],
    }


def test_spider_constants(spider):
    """Kiểm tra các hằng số cấu hình của Spider theo đúng yêu cầu đề bài."""
    assert spider.name == "chotot_laptop"
    assert (
        spider.base_api
        == "https://gateway.chotot.com/v1/public/ad-listing?limit=50&cg=5030&st=s,k&key_param_included=true&o={offset}"
    )
    assert spider.MAX_ITEMS == 2000
    assert spider.PAGE_SIZE == 50


def test_start_requests(spider):
    """Kiểm tra start_requests sinh ra request đầu tiên với offset = 0."""
    requests = list(spider.start_requests())
    assert len(requests) == 1
    req = requests[0]
    assert isinstance(req, Request)
    assert "o=0" in req.url
    assert req.meta.get("offset") == 0


def test_extract_item_full_fields(spider, sample_ad):
    """Kiểm tra trích xuất đầy đủ 19 trường dữ liệu theo quy cách."""
    item = spider.extract_item(sample_ad)
    assert isinstance(item, ProductDetailItem)

    # 1. price
    assert item["price"] == 12800000

    # 2. title (lấy từ 'subject')
    assert item["title"] == "Macbook Pro CORE I9 - 9880H với SSD 1TB"

    # 3. description (lấy từ 'body')
    assert item["description"] == "💥-----BÁN 12tr8 Macbook Pro 16 2019 MÁY CHÍNH CHỦ..."

    # 4. cpu (textual từ params 'pc_cpu')
    assert item["cpu"] == "Intel Core i9"

    # 5. ram (textual từ params 'pc_ram')
    assert item["ram"] == "16 GB"

    # 6. hard_drive (lấy từ 'pc_drive_capacity')
    assert item["hard_drive"] == "1 TB"

    # 7. hard_drive_type (lấy từ 'pc_drive_type')
    assert item["hard_drive_type"] == "SSD"

    # 8. seller_rating (lấy từ 'average_rating')
    assert item["seller_rating"] == 4.5

    # 9. seller_sold_count (lấy từ 'sold_ads')
    assert item["seller_sold_count"] == 250

    # 10. pc_brand (lấy từ 'pc_brand')
    assert item["pc_brand"] == 3

    # 11. pc_cpu (lấy từ 'pc_cpu')
    assert item["pc_cpu"] == 12

    # 12. pc_ram (lấy từ 'pc_ram')
    assert item["pc_ram"] == 7

    # 13. region_name (lấy từ 'region_name_v3')
    assert item["region_name"] == "TP Hồ Chí Minh"

    # 14. ward_name (lấy từ 'ward_name_v3')
    assert item["ward_name"] == "Phường Hạnh Thông"

    # 15. elt_condition (lấy từ 'elt_condition')
    assert item["elt_condition"] == 2

    # 16. elt_warranty (lấy từ 'elt_warranty')
    assert item["elt_warranty"] == 6

    # 17. elt_origin (lấy từ 'elt_origin')
    assert item["elt_origin"] == 8

    # 18. laptop_screen_size (lấy từ 'laptop_screen_size')
    assert item["laptop_screen_size"] == 5

    # 19. list_id (lấy từ 'list_id')
    assert item["list_id"] == 135459559

    # Metadata bổ trợ
    assert item["url"] == "https://www.chotot.com/135459559.htm"
    assert "crawled_at" in item


def test_extract_item_fallbacks(spider):
    """Kiểm tra các trường hợp fallback: thiếu params, sold_ads trong seller_info."""
    ad_minimal = {
        "list_id": 999999,
        "price": 5000000,
        "subject": "Laptop cu gia re",
        "body": "May con dung tot",
        "pc_drive_capacity": 10,
        "pc_drive_type": 1,
        "seller_info": {"sold_ads": 42},
    }

    item = spider.extract_item(ad_minimal)
    assert item["list_id"] == 999999
    assert item["title"] == "Laptop cu gia re"
    assert item["description"] == "May con dung tot"
    assert item["cpu"] is None
    assert item["ram"] is None
    assert item["hard_drive"] == 10  # fallback trực tiếp từ pc_drive_capacity
    assert item["hard_drive_type"] == 1  # fallback trực tiếp từ pc_drive_type
    assert item["seller_sold_count"] == 42  # lấy từ seller_info
    assert item["seller_rating"] is None
    assert item["elt_warranty"] is None


def test_parse_yields_items_and_next_request(spider, sample_ad):
    """Kiểm tra hàm parse sinh đủ các Item và Request phân trang tiếp theo."""
    payload = {
        "total": 500,
        "ads": [sample_ad, sample_ad],
    }
    mock_request = Request(url=spider.base_api.format(offset=0), meta={"offset": 0})
    mock_response = TextResponse(
        url=spider.base_api.format(offset=0),
        status=200,
        body=json.dumps(payload).encode("utf-8"),
        request=mock_request,
    )

    results = list(spider.parse(mock_response))
    items = [r for r in results if isinstance(r, ProductDetailItem)]
    requests = [r for r in results if isinstance(r, Request)]

    assert len(items) == 2
    assert len(requests) == 1
    next_req = requests[0]
    assert next_req.meta.get("offset") == 50
    assert "o=50" in next_req.url


def test_parse_stops_at_max_items(spider, sample_ad):
    """Kiểm tra parse dừng lại khi next_offset >= MAX_ITEMS."""
    payload = {
        "total": 5000,
        "ads": [sample_ad],
    }
    # Offset hiện tại là 1950, next_offset sẽ là 2000 (đạt MAX_ITEMS = 2000)
    mock_request = Request(url=spider.base_api.format(offset=1950), meta={"offset": 1950})
    mock_response = TextResponse(
        url=spider.base_api.format(offset=1950),
        status=200,
        body=json.dumps(payload).encode("utf-8"),
        request=mock_request,
    )

    results = list(spider.parse(mock_response))
    items = [r for r in results if isinstance(r, ProductDetailItem)]
    requests = [r for r in results if isinstance(r, Request)]

    assert len(items) == 1
    # Không được sinh thêm request tiếp theo vì đã đạt giới hạn MAX_ITEMS
    assert len(requests) == 0


def test_parse_stops_when_no_more_ads(spider):
    """Kiểm tra parse dừng lại khi không còn bài đăng hoặc offset >= total."""
    payload = {
        "total": 30,
        "ads": [],
    }
    mock_request = Request(url=spider.base_api.format(offset=0), meta={"offset": 0})
    mock_response = TextResponse(
        url=spider.base_api.format(offset=0),
        status=200,
        body=json.dumps(payload).encode("utf-8"),
        request=mock_request,
    )

    results = list(spider.parse(mock_response))
    assert len(results) == 0


def test_parse_malformed_json(spider):
    """Kiểm tra xử lý an toàn khi phản hồi từ server không phải JSON hợp lệ."""
    mock_request = Request(url=spider.base_api.format(offset=0), meta={"offset": 0})
    mock_response = TextResponse(
        url=spider.base_api.format(offset=0),
        status=200,
        body=b"<html>Bad Gateway</html>",
        request=mock_request,
    )

    results = list(spider.parse(mock_response))
    assert len(results) == 0
