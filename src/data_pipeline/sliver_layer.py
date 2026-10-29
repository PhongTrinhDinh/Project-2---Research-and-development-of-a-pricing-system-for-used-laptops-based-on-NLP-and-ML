import os
from src.utils.logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, regexp_extract, lower, trim, concat_ws, when

# logging setup
logger = get_logger(__name__, log_file="logs/etl_pipeline/silver.log")

def process_silver_layer():
    spark = SparkSession.builder.appName("SilverLayer_Processing").getOrCreate()
    
    bronze_dir = "./data/raw/bronze_layer"
    silver_dir = "./data/processed/silver_layer"
    
    logger.info("Reading data from Bronze...")
    df_bronze = spark.read.parquet(bronze_dir)
    
    df_flat = df_bronze.select(
        col("list_id").cast("string").alias("post_id"), # ID nên để dạng chuỗi
        col("title"),
        col("description"),
        col("price").cast("float").alias("target_price"), # Đã là số sẵn, chỉ cần ép kiểu Float cho ML
    
        # Các thông số kỹ thuật dạng chuỗi (String)
        col("cpu"),
        col("ram"),
        col("hard_drive"),
        col("hard_drive_type"),
    
        # Các thông số đã được mã hóa thành số (Categorical encoded)
        col("pc_brand").cast("integer"),
        col("pc_cpu").cast("integer"),
        col("pc_ram").cast("integer"),
        col("laptop_screen_size").cast("integer"),
        col("elt_condition").cast("integer"),
        col("elt_origin").cast("integer"),
        col("elt_warranty").cast("integer"), # Cột này có giá trị null
    
        # Thông tin địa lý & người bán
        col("region_name"),
        col("ward_name"),
        col("seller_rating").cast("float"),
        col("seller_sold_count").cast("integer"),
    
        # Metadata
        col("crawled_at").cast("timestamp"),
        col("url")
    )
    
    df_clean = df_flat.withColumn("title_clean", lower(trim(col("title")))) \
                      .withColumn("description_clean", lower(col("description"))) \
                      .withColumn("description_clean", regexp_replace(col("description_clean"), r"(?U)[^\w\s]", " ")) \
                      .withColumn("description_clean", regexp_replace(col("description_clean"), r"\s+", " ")) \
                      .withColumn("description_clean", trim(col("description_clean")))
                      
    df_clean = df_clean.fillna({
        "elt_warranty": -1,
        "seller_rating": 0.0,
        "seller_sold_count": 0
    })
    
    df_extracted = df_clean.withColumn("full_text", concat_ws(" ", col("title_clean"), col("description_clean"))) \
                           .withColumn("text_lower", concat_ws(" ", lower(col("title")), lower(col("description"))))  # chỉ chuyển chữ thường, giữ nguyên ký tự (%, +, /, ") để trích xuất đặc trưng
    
    df_features = df_extracted \
        .withColumn("brand_name", regexp_extract(col("text_lower"), r"(hp|dell|asus|acer|lenovo|apple|macbook|msi|thinkpad|surface)", 1)) \
        .withColumn("line_model", regexp_extract(col("text_lower"), r"(elitebook\s\d+|latitude\s\d+|xps\s\d+|thinkpad\s[a-z]\d+|macbook\s(?:pro|air))", 1)) \
        .withColumn("sku_gen", regexp_extract(col("text_lower"), r"(i[3579]-?\s?\d{3,5}[hqpkgu]*|ryzen\s?\d\s?-?\d{4,5}[hu]*|m[123]\s?(?:pro|max)?)", 1)) \
        .withColumn("gpu", regexp_extract(col("text_lower"), r"(rtx\s?\d{4}(?:\s?ti)?|gtx\s?\d{4}(?:\s?ti)?|iris\s?xe|uhd\sgraphics|radeon)", 1)) \
        .withColumn("screen_size", regexp_extract(col("text_lower"), r"(\d{2}(?:\.\d{1,2})?)\s?(?:inch|\")", 1)) \
        .withColumn("resolution", regexp_extract(col("text_lower"), r"(fhd\+|fhd|full\shd\+|full\shd|2k|3k|4k|1920x1[02]80|2560x1440)", 1)) \
        .withColumn("touch_sensitive", when(col("text_lower").rlike("cảm ứng|touch"), 1).otherwise(0)) \
        .withColumn("year_production", regexp_extract(col("text_lower"), r"(?:đời|năm|sản xuất)\s?(\d{4})", 1)) \
        .withColumn("likely_new", regexp_extract(col("text_lower"), r"(100%|9[0-9]%|like\s?new|mới\s?100%|nguyên seal|new\s?seal)", 1)) \
        .withColumn("origin_source", regexp_extract(col("text_lower"), r"(chính hãng|xách tay|mỹ|nhật|hàn|vn/a|ll/a)", 1)) \
        .withColumn("repair_status", when(col("text_lower").rlike("zin|nguyên bản|chưa sửa"), "zin").when(col("text_lower").rlike("đã sửa|thay"), "repaired").otherwise("unknown")) \
        .withColumn("battery_status", regexp_extract(col("text_lower"), r"pin\s?(100%|9\d%|\d{1,2}h|trâu|chai|good|tốt|zin)", 1)) \
        .withColumn("remaining_warranty", regexp_extract(col("text_lower"), r"bảo hành\s?(\d+\s?tháng|còn|hết)", 1)) \
        .withColumn("bundles", regexp_extract(lower(col("description")), r"(?:tặng kèm|tặng|quà tặng)[\s:]+([^\n]+)", 1))
    
    # Loại bỏ các cột văn bản trung gian, chỉ dùng cho bước trích xuất đặc trưng
    df_silver = df_features.drop(
        "title",
        "description",
        "title_clean",
        "description_clean",
        "full_text",
        "text_lower"
    )

            