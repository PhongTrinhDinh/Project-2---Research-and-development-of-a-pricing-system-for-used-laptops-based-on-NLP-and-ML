import os
from src.utils.logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, regexp_replace, regexp_extract, trim, lower, current_date, year, months_between, to_date
    
from src.data_pipeline.silver_patterns import (
    normalize_text_column,
    extract_safe,
    extract_pref,
    BRAND_DIRECT_PATTERN,
    PRODUCT_LINE_PATTERN,
    BRAND_FROM_LINE_CONDITIONS,
    INTEL_MODEL_PATTERN,
    INTEL_GEN_PHRASE_PATTERN,
    INTEL_ULTRA_PATTERN,
    AMD_MODEL_PATTERN,
    APPLE_M_PATTERN,
    GPU_DISCRETE_PATTERN,
    GPU_INTEGRATED_PATTERN,
    SCREEN_SIZE_PATTERN,
    RESOLUTION_NUM_PATTERN,
    RESOLUTION_TEXT_PATTERN,
    TOUCH_POS_PATTERN,
    TOUCH_NEG_PATTERN,
    NEW_SEAL_PATTERN,
    LIKE_NEW_PATTERN,
    CONDITION_PCT_PATTERN,
    ORIGIN_COUNTRY_PATTERN,
    ORIGIN_TYPE_PATTERN,
    BATTERY_PCT_PATTERN,
    BATTERY_CYCLES_PATTERN,
    BATTERY_HOURS_PATTERN,
    BATTERY_WORN_PATTERN,
    BATTERY_REPLACED_PATTERN,
    BATTERY_GOOD_PATTERN,
    WARRANTY_MONTHS_PATTERN,
    WARRANTY_YEARS_PATTERN,
    WARRANTY_EXPIRED_PATTERN,
    WARRANTY_TYPE_PATTERN,
    UPGRADE_PATTERN,
    REPAIR_ORIGINAL_PATTERN,
    REPAIR_FIXED_PATTERN,
    YEAR_PRODUCTION_PATTERN,
    YEAR_PURCHASE_PATTERN,
    BUNDLES_PATTERN,
)

logger = get_logger(__name__, log_file="logs/etl_pipeline/silver.log")


def process_silver_layer(spark: SparkSession = None):
    is_owner = False
    if spark is None:
        spark = SparkSession.builder.appName("SilverLayer_Processing").getOrCreate()
        is_owner = True

    bronze_dir = os.getenv("DATALAKE_BRONZE_DIR", "./data/raw/bronze_layer")
    silver_dir = os.getenv("DATALAKE_SILVER_DIR", "./data/processed/silver_layer")

    logger.info(f"Reading data from Bronze: {bronze_dir}")

    try:
        df_bronze = spark.read.parquet(bronze_dir).dropDuplicates(["list_id"])

        # 1. Ép kiểu và chọn lọc dữ liệu từ Bronze
        df_flat = df_bronze.select(
            col("list_id").cast("string").alias("post_id"),
            col("title"),
            col("description"),
            col("price").cast("float").alias("target_price"),

            # Thông số dạng chuỗi từ nguồn
            col("cpu").alias("raw_cpu"),
            col("ram").alias("raw_ram"),
            col("hard_drive").alias("raw_hard_drive"),
            col("hard_drive_type").alias("raw_hard_drive_type"),

            # Mã số phân loại
            col("pc_brand").cast("integer"),
            col("pc_cpu").cast("integer"),
            col("pc_ram").cast("integer"),
            col("laptop_screen_size").cast("integer"),
            col("elt_condition").cast("integer"),
            col("elt_origin").cast("integer"),
            col("elt_warranty").cast("integer"),

            # Thông tin địa lý & người bán
            col("region_name"),
            col("ward_name"),
            col("seller_rating").cast("float"),
            col("seller_sold_count").cast("integer"),

            # Metadata
            col("crawled_at").cast("timestamp"),
            col("url")
        )

        # 2. Xử lý giá trị thiếu (Missing values) chuẩn ML:
        # - GIỮ NGUYÊN NULL cho seller_rating (không ép về 0.0 để tránh hiểu lầm người mới là lừa đảo)
        # - Tạo cờ nhị phân is_seller_rating_missing
        df_clean = df_flat.withColumn(
            "is_seller_rating_missing",
            when(col("seller_rating").isNull(), 1).otherwise(0)
        ).fillna({
            "elt_warranty": -1,
            "seller_sold_count": 0
        })

        # 3. Chuẩn hóa văn bản: NFC, NBSP, lower, gộp khoảng trắng
        df_norm = df_clean \
            .withColumn("title_norm", normalize_text_column(col("title"))) \
            .withColumn("desc_norm", normalize_text_column(col("description")))

        title_c = col("title_norm")
        desc_c = col("desc_norm")

        # 4. Trích xuất đặc trưng có truy vết nguồn gốc (_src cho 4 trường quan trọng)
        # 4.1. Product Line & Brand Name
        prod_line_val, prod_line_src = extract_pref(title_c, desc_c, PRODUCT_LINE_PATTERN, 0, return_source=True)
        brand_direct_val, brand_direct_src = extract_pref(title_c, desc_c, BRAND_DIRECT_PATTERN, 0, return_source=True)

        # Suy luận hãng từ dòng máy nếu hãng trực tiếp bị null
        brand_inferred = lit(None)
        for pattern_cond, brand_label in reversed(BRAND_FROM_LINE_CONDITIONS):
            brand_inferred = when(prod_line_val.rlike(pattern_cond), lit(brand_label)).otherwise(brand_inferred)

        brand_final = coalesce(brand_direct_val, brand_inferred)
        brand_final_src = when(brand_direct_val.isNotNull(), brand_direct_src) \
            .when(brand_inferred.isNotNull(), lit("inferred_from_line")) \
            .otherwise(lit(None))

        # 4.2. CPU: Model, Vendor, Tier, Gen
        intel_m = extract_pref(title_c, desc_c, INTEL_MODEL_PATTERN, 1)
        intel_u = extract_pref(title_c, desc_c, INTEL_ULTRA_PATTERN, 0)
        amd_m = extract_pref(title_c, desc_c, AMD_MODEL_PATTERN, 1)
        apple_m = extract_pref(title_c, desc_c, APPLE_M_PATTERN, 1)

        # Ưu tiên theo độ cụ thể và nguồn
        cpu_model_val = coalesce(intel_m, intel_u, amd_m, apple_m)
        _, cpu_model_src = extract_pref(
            title_c, desc_c,
            f"(?:{INTEL_MODEL_PATTERN}|{INTEL_ULTRA_PATTERN}|{AMD_MODEL_PATTERN}|{APPLE_M_PATTERN})",
            0,
            return_source=True
        )

        cpu_vendor_val = (
            when(intel_m.isNotNull() | intel_u.isNotNull(), lit("intel"))
            .when(amd_m.isNotNull(), lit("amd"))
            .when(apple_m.isNotNull(), lit("apple"))
            .otherwise(lit(None))
        )

        # CPU Generation (int)
        gen_from_model = (
            when(cpu_model_val.rlike(r"i[3579]-?1[0-5]\d{3}"), regexp_extract(cpu_model_val, r"i[3579]-?(1[0-5])", 1).cast("integer"))
            .when(cpu_model_val.rlike(r"i[3579]-?[2-9]\d{3}"), regexp_extract(cpu_model_val, r"i[3579]-?([2-9])", 1).cast("integer"))
            .when(cpu_model_val.rlike(r"ryzen\s*[3579]\s*(?:pro\s*)?(\d)\d{3}"), regexp_extract(cpu_model_val, r"ryzen\s*[3579]\s*(?:pro\s*)?(\d)\d{3}", 1).cast("integer"))
            .otherwise(lit(None))
        )
        gen_phrase = extract_pref(title_c, desc_c, INTEL_GEN_PHRASE_PATTERN, 1)
        cpu_gen_val = coalesce(gen_from_model, gen_phrase.cast("integer"))

        # 4.3. GPU: Model, Type
        gpu_disc = extract_pref(title_c, desc_c, GPU_DISCRETE_PATTERN, 1)
        gpu_integ = extract_pref(title_c, desc_c, GPU_INTEGRATED_PATTERN, 1)
        gpu_model_val = coalesce(gpu_disc, gpu_integ)
        _, gpu_model_src = extract_pref(
            title_c, desc_c,
            f"(?:{GPU_DISCRETE_PATTERN}|{GPU_INTEGRATED_PATTERN})",
            0,
            return_source=True
        )
        gpu_type_val = (
            when(gpu_disc.isNotNull(), lit("discrete"))
            .when(gpu_integ.isNotNull(), lit("integrated"))
            .otherwise(lit(None))
        )

        # 4.4. Screen Size & Resolution
        screen_raw = extract_pref(title_c, desc_c, SCREEN_SIZE_PATTERN, 1)
        screen_standardized = regexp_replace(screen_raw, ",", ".").cast("float")
        screen_size_inch = when(
            (screen_standardized >= 10.0) & (screen_standardized <= 18.5),
            screen_standardized
        ).otherwise(lit(None))

        res_num_w = extract_pref(title_c, desc_c, RESOLUTION_NUM_PATTERN, 1).cast("integer")
        res_num_h = extract_pref(title_c, desc_c, RESOLUTION_NUM_PATTERN, 2).cast("integer")
        res_text = extract_pref(title_c, desc_c, RESOLUTION_TEXT_PATTERN, 1)

        resolution_val = (
            when((res_num_w == 1920) & (res_num_h == 1080), lit("fhd"))
            .when((res_num_w == 1920) & (res_num_h == 1200), lit("fhd+"))
            .when((res_num_w == 2560) & (res_num_h == 1440), lit("qhd/2k"))
            .when((res_num_w == 2560) & (res_num_h == 1600), lit("qhd+/2k+"))
            .when((res_num_w == 2880) & (res_num_h == 1800), lit("3k"))
            .when((res_num_w == 3840) & ((res_num_h == 2160) | (res_num_h == 2400)), lit("4k/uhd"))
            .when(res_text.isNotNull(), res_text)
            .otherwise(lit(None))
        )

        # 4.5. Touch Sensitive (1, 0, NULL)
        touch_pos = extract_pref(title_c, desc_c, TOUCH_POS_PATTERN, 0)
        touch_neg = extract_pref(title_c, desc_c, TOUCH_NEG_PATTERN, 0)
        touch_sensitive_val = (
            when(touch_pos.isNotNull(), 1)
            .when(touch_neg.isNotNull(), 0)
            .otherwise(lit(None))
        )

        # 4.6. Condition & Likely New
        is_new_seal_val = when(extract_pref(title_c, desc_c, NEW_SEAL_PATTERN, 0).isNotNull(), 1).otherwise(lit(None))
        is_like_new_val = when(extract_pref(title_c, desc_c, LIKE_NEW_PATTERN, 0).isNotNull(), 1).otherwise(lit(None))
        condition_pct_raw = extract_pref(title_c, desc_c, CONDITION_PCT_PATTERN, 1).cast("integer")
        condition_pct_val = when(
            (condition_pct_raw >= 50) & (condition_pct_raw <= 100),
            condition_pct_raw
        ).otherwise(lit(None))

        # 4.7. Origin
        origin_country_val = extract_pref(title_c, desc_c, ORIGIN_COUNTRY_PATTERN, 1)
        origin_type_val = extract_pref(title_c, desc_c, ORIGIN_TYPE_PATTERN, 1)

        # 4.8. Battery
        bat_pct_raw = extract_pref(title_c, desc_c, BATTERY_PCT_PATTERN, 1).cast("integer")
        battery_pct_val = when((bat_pct_raw >= 10) & (bat_pct_raw <= 100), bat_pct_raw).otherwise(lit(None))
        battery_cycles_val = extract_pref(title_c, desc_c, BATTERY_CYCLES_PATTERN, 1).cast("integer")
        battery_hours_raw = regexp_replace(extract_pref(title_c, desc_c, BATTERY_HOURS_PATTERN, 1), ",", ".").cast("float")
        battery_hours_val = when((battery_hours_raw >= 0.5) & (battery_hours_raw <= 24.0), battery_hours_raw).otherwise(lit(None))

        bat_worn = extract_pref(title_c, desc_c, BATTERY_WORN_PATTERN, 0)
        bat_replaced = extract_pref(title_c, desc_c, BATTERY_REPLACED_PATTERN, 0)
        bat_good = extract_pref(title_c, desc_c, BATTERY_GOOD_PATTERN, 0)
        battery_condition_val = (
            when(bat_worn.isNotNull(), lit("worn"))
            .when(bat_replaced.isNotNull(), lit("replaced"))
            .when(bat_good.isNotNull(), lit("good"))
            .otherwise(lit(None))
        )

        # 4.9. Warranty
        w_m = extract_pref(title_c, desc_c, WARRANTY_MONTHS_PATTERN, 1).cast("integer")
        w_y = extract_pref(title_c, desc_c, WARRANTY_YEARS_PATTERN, 1).cast("integer")
        w_exp = extract_pref(title_c, desc_c, WARRANTY_EXPIRED_PATTERN, 0)
        warranty_months_val = (
            when(w_exp.isNotNull(), 0)
            .when(w_m.isNotNull(), w_m)
            .when(w_y.isNotNull(), w_y * 12)
            .otherwise(lit(None))
        )
        warranty_type_val = extract_pref(title_c, desc_c, WARRANTY_TYPE_PATTERN, 1)

        # 4.10. Repair & Upgrade
        is_upgraded_val = when(extract_pref(title_c, desc_c, UPGRADE_PATTERN, 0).isNotNull(), 1).otherwise(lit(None))
        rep_orig = extract_pref(title_c, desc_c, REPAIR_ORIGINAL_PATTERN, 0)
        rep_fixed = extract_pref(title_c, desc_c, REPAIR_FIXED_PATTERN, 0)
        repair_status_val = (
            when(rep_orig.isNotNull(), lit("original"))
            .when(rep_fixed.isNotNull(), lit("repaired"))
            .otherwise(lit(None))
        )

        # 4.11. Year
        year_prod_raw = extract_pref(title_c, desc_c, YEAR_PRODUCTION_PATTERN, 1).cast("integer")
        curr_yr = 2026
        year_production_val = when(
            (year_prod_raw >= 2010) & (year_prod_raw <= curr_yr + 1),
            year_prod_raw
        ).otherwise(lit(None))
        year_purchase_val = extract_pref(title_c, desc_c, YEAR_PURCHASE_PATTERN, 1).cast("integer")

        # 4.12. Bundles (chỉ từ mô tả)
        bundles_val = extract_safe(desc_c, BUNDLES_PATTERN, 1)

        # 5. Gán toàn bộ đặc trưng vào DataFrame
        df_features = df_norm \
            .withColumn("product_line", prod_line_val) \
            .withColumn("product_line_src", prod_line_src) \
            .withColumn("brand_name", brand_final) \
            .withColumn("brand_name_src", brand_final_src) \
            .withColumn("cpu_vendor", cpu_vendor_val) \
            .withColumn("cpu_model", cpu_model_val) \
            .withColumn("cpu_model_src", cpu_model_src) \
            .withColumn("cpu_gen", cpu_gen_val) \
            .withColumn("gpu_type", gpu_type_val) \
            .withColumn("gpu_model", gpu_model_val) \
            .withColumn("gpu_model_src", gpu_model_src) \
            .withColumn("screen_size_inch", screen_size_inch) \
            .withColumn("resolution", resolution_val) \
            .withColumn("touch_sensitive", touch_sensitive_val) \
            .withColumn("is_new_seal", is_new_seal_val) \
            .withColumn("is_like_new", is_like_new_val) \
            .withColumn("condition_pct", condition_pct_val) \
            .withColumn("origin_type", origin_type_val) \
            .withColumn("origin_country", origin_country_val) \
            .withColumn("battery_pct", battery_pct_val) \
            .withColumn("battery_cycles", battery_cycles_val) \
            .withColumn("battery_hours", battery_hours_val) \
            .withColumn("battery_condition", battery_condition_val) \
            .withColumn("warranty_months", warranty_months_val) \
            .withColumn("warranty_type", warranty_type_val) \
            .withColumn("is_upgraded", is_upgraded_val) \
            .withColumn("repair_status", repair_status_val) \
            .withColumn("year_production", year_production_val) \
            .withColumn("year_purchase", year_purchase_val) \
            .withColumn("bundles", bundles_val)

        # 6. Loại bỏ các cột văn bản gốc và trung gian
        df_silver = df_features.drop(
            "title",
            "description",
            "title_norm",
            "desc_norm"
        )

        logger.info(f"Writing data to Silver Parquet: {silver_dir}")
        os.makedirs(silver_dir, exist_ok=True)
        df_silver.write.format("parquet") \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .save(silver_dir)

        logger.info("Successfully finished Silver layer ETL pipeline.")
        return df_silver

    except Exception as e:
        logger.error(f"Error in Silver layer ETL: {e}")
        raise
    finally:
        if is_owner:
            logger.info("Stopping SparkSession...")
            spark.stop()


if __name__ == "__main__":
    process_silver_layer()

