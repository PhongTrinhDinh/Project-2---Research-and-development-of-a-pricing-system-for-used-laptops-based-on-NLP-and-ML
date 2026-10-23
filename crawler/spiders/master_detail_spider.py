import scrapy
from datetime import datetime
from items import ProductDetailItem

class MasterDetailSpider(scrapy.Spider):
    name = "chotot_laptop"
    
    # URL khởi đầu cho trang danh sách (Master)
    start_urls = ["https://www.chotot.com/mua-ban-laptop-ha-noi?event_source=ct_home_highlight_category"]

    def parse(self, response):
        """
        MASTER STAGE:
        Bóc tách danh sách liên kết dẫn tới trang chi tiết và tìm trang tiếp theo.
        """
        # 1. Trích xuất các liên kết đến trang chi tiết
        detail_links = response.css("li a[href*='.htm']::attr(href)").getall()
        self.logger.info(f"Catched {len(detail_links)} links...")
        for link in detail_links:
            # Gửi request sang trang detail, truyền callback về parse_detail
            yield response.follow(
                link, 
                callback=self.parse_detail,
                # Giữ nguyên priority thấp hơn phân trang để không dồn ứ RAM
                priority=10 
            )

        # 2. Xử lý phân trang (Next Page)
        next_page = response.css("a[class*='Paging_redirectPageBtn']::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page, 
                callback=self.parse,
                priority=20  # Ưu tiên cao hơn để quét danh sách theo chiều sâu
            )

    def parse_detail(self, response):
        """
        DETAIL STAGE:
        Thu thập thông tin cụ thể từ trang mục tiêu.
        """
        item = ProductDetailItem()
        item["url"] = response.url
        item["crawled_at"] = datetime.utcnow().isoformat()
        
        # 1. Tiêu đề, Giá, Mô tả
        item["title"] = response.xpath("//h1//text()").get(default="").strip()
        item["price"] = response.xpath("//b[contains(text(), 'đ')]/text()").get(default="").strip()
        
        location_parts = response.xpath("//div[contains(@class, 'impob58')]/following-sibling::div//text()").getall()
        item["location"] = " ".join([text.strip() for text in location_parts if text.strip()])
        
        desc_parts = response.xpath("//*[@itemprop='description']//text()").getall()
        if not desc_parts:
            desc_parts = response.xpath("//div[contains(@class, 'c2yxoiv')]//p//text()").getall()
        item["description"] = "\n".join([p.strip() for p in desc_parts if p.strip()])

        # 2. Bóc tách linh hoạt các thông số kỹ thuật (Spec Grid)
        item["brand"] = response.xpath("//div[contains(text(), 'Dòng máy')]/following-sibling::div//text()").get(default="").strip()
        item["line_product"] = response.xpath("//div[contains(text(), 'Hãng')]/following-sibling::div//text()").get(default="").strip()
        item["device_condition"] = response.xpath("//div[contains(text(), 'Tình trạng')]/following-sibling::div//text()").get(default="").strip()
        item["screen_size"] = response.xpath("//div[contains(text(), 'Kích cỡ màn hình')]/following-sibling::div//text()").get(default="").strip()
        item["cpu"] = response.xpath("//div[contains(text(), 'Bộ vi xử lý')]/following-sibling::div//text()").get(default="").strip()
        item["ram"] = response.xpath("//div[contains(text(), 'RAM')]/following-sibling::div//text()").get(default="").strip()
        item["graphic_card"] = response.xpath("//div[contains(text(), 'Card màn hình')]/following-sibling::div//text()").get(default="").strip()
        item["hard_drive"] = response.xpath("//div[contains(text(), 'Ổ cứng')]/following-sibling::div//text()").get(default="").strip()
        item["hard_drive_type"] = response.xpath("//div[contains(text(), 'Loại ổ cứng')]/following-sibling::div//text()").get(default="").strip()

        yield item