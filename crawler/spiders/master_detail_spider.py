import json
import scrapy
from datetime import datetime, timezone

try:
    from crawler.items import ProductDetailItem
except ImportError:
    from items import ProductDetailItem


class MasterDetailSpider(scrapy.Spider):
    name = "chotot_laptop"

    base_api = "https://gateway.chotot.com/v1/public/ad-listing?limit=50&cg=5030&st=s,k&key_param_included=true&o={offset}"
    MAX_ITEMS = 2000
    PAGE_SIZE = 50

    async def start(self):
        """Khởi tạo request đầu tiên (tương thích Scrapy >= 2.13)."""
        yield scrapy.Request(
            url=self.base_api.format(offset=0),
            callback=self.parse,
            meta={"offset": 0}
        )

    def start_requests(self):
        """Khởi tạo request đầu tiên (tương thích Scrapy < 2.13 và testing)."""
        yield scrapy.Request(
            url=self.base_api.format(offset=0),
            callback=self.parse,
            meta={"offset": 0}
        )


    def extract_item(self, ad):
        """
        Trích xuất đầy đủ 19 trường dữ liệu và metadata từ một quảng cáo (ad) trong JSON API.
        """
        item = ProductDetailItem()

        # Metadata
        list_id = ad.get("list_id")
        item["list_id"] = list_id
        item["url"] = f"https://www.chotot.com/{list_id}.htm" if list_id else ""
        item["crawled_at"] = datetime.now(timezone.utc).isoformat()

        # Core fields: title từ 'subject', description từ 'body'
        item["price"] = ad.get("price")
        subject = ad.get("subject")
        item["title"] = subject.strip() if isinstance(subject, str) else subject
        body = ad.get("body")
        item["description"] = body.strip() if isinstance(body, str) else body

        # Trích xuất thông số kỹ thuật dạng text từ danh sách params (key_param_included=true)
        params_list = ad.get("params") or []
        param_dict = {
            p.get("id"): p.get("value")
            for p in params_list
            if isinstance(p, dict) and p.get("id")
        }

        # cpu, ram dạng text nếu có trong params, hoặc fallback mã số
        item["cpu"] = param_dict.get("pc_cpu")
        item["ram"] = param_dict.get("pc_ram")

        # hard_drive lấy từ "pc_drive_capacity", hard_drive_type lấy từ "pc_drive_type"
        item["hard_drive"] = param_dict.get("pc_drive_capacity") or ad.get("pc_drive_capacity")
        item["hard_drive_type"] = param_dict.get("pc_drive_type") or ad.get("pc_drive_type")

        # Đánh giá và số lượt bán người bán
        item["seller_rating"] = ad.get("average_rating")
        seller_sold = ad.get("sold_ads")
        if seller_sold is None and isinstance(ad.get("seller_info"), dict):
            seller_sold = ad.get("seller_info", {}).get("sold_ads")
        item["seller_sold_count"] = seller_sold

        # Các mã thuộc tính danh mục & địa chỉ
        item["pc_brand"] = ad.get("pc_brand")
        item["pc_cpu"] = ad.get("pc_cpu")
        item["pc_ram"] = ad.get("pc_ram")
        item["region_name"] = ad.get("region_name_v3")
        item["ward_name"] = ad.get("ward_name_v3")
        item["elt_condition"] = ad.get("elt_condition")
        item["elt_warranty"] = ad.get("elt_warranty")
        item["elt_origin"] = ad.get("elt_origin")
        item["laptop_screen_size"] = ad.get("laptop_screen_size")

        return item

    def parse(self, response):
        """
        Bóc tách dữ liệu từ API JSON và điều hướng phân trang tự động bằng offset.
        """
        try:
            data = response.json() if hasattr(response, "json") else json.loads(response.text)
        except Exception as e:
            self.logger.error(f"Lỗi phân tích JSON tại URL {response.url}: {e}")
            return

        ads = data.get("ads", [])
        total = data.get("total", 0)
        current_offset = response.meta.get("offset", 0)

        self.logger.info(
            f"Đang crawl offset={current_offset} | Số lượng ads={len(ads)} | Tổng ads ước tính={total}"
        )

        for ad in ads:
            yield self.extract_item(ad)

        # Xử lý phân trang bằng offset
        next_offset = current_offset + self.PAGE_SIZE
        if ads and next_offset < total and next_offset < self.MAX_ITEMS:
            yield scrapy.Request(
                url=self.base_api.format(offset=next_offset),
                callback=self.parse,
                meta={"offset": next_offset}
            )
        else:
            self.logger.info(
                f"Dừng crawl tại offset={next_offset} (Đã hết tin hoặc đạt MAX_ITEMS={self.MAX_ITEMS})"
            )