import os
from src.utils.logger import get_logger
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    lower,
    trim,
    regexp_extract,
)
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    Imputer,
)

logger = get_logger(__name__, log_file="logs/etl_pipeline/gold.log")


def process_gold_layer(spark: SparkSession = None):
    is_owner = False
    if spark is None:
        spark = SparkSession.builder.appName("GoldLayer_FeatureEngineering").getOrCreate()
        is_owner = True

    silver_dir = os.getenv("DATALAKE_SILVER_DIR", "./data/processed/silver_layer")
    gold_dir = os.getenv("DATALAKE_GOLD_DIR", "./data/features/gold_layer")
    model_artifact_dir = os.getenv("MODEL_TRANSFORMER_DIR", "./models/transformers")

    logger.info(f"Reading data from Silver layer: {silver_dir}")

    try:
        df_silver = spark.read.parquet(silver_dir)

        # ----------------------------------------------------------------------
        # 1. Lọc bỏ Outliers & Dữ liệu rác của Biến mục tiêu (target_price)
        # ----------------------------------------------------------------------
        # Trên Chợ Tốt có rất nhiều tin đăng giá ảo: 0đ, 1.000đ (thương lượng), hoặc 999 triệu.
        # Lọc trong khoảng giá thực tế cho laptop: từ 500.000 VNĐ đến 200.000.000 VNĐ.
        df_filtered = df_silver.filter(
            col("target_price").isNotNull()
            & (col("target_price") >= 500000.0)
            & (col("target_price") <= 200000000.0)
        )

        # ----------------------------------------------------------------------
        # 2. Khôi phục các đặc trưng phần cứng cốt lõi: RAM & Ổ cứng (Storage)
        # ----------------------------------------------------------------------
        # Trích xuất dung lượng RAM (GB)
        ram_from_raw = regexp_extract(lower(col("raw_ram")), r"(\d+)\s*gb", 1).cast("float")
        df_hw = df_filtered.withColumn("ram_gb", ram_from_raw)

        # Trích xuất dung lượng Ổ cứng (GB): quy đổi TB -> GB (1 TB = 1024 GB)
        storage_tb = regexp_extract(lower(col("raw_hard_drive")), r"(\d+(?:\.\d+)?)\s*tb", 1).cast("float") * 1024.0
        storage_gb = regexp_extract(lower(col("raw_hard_drive")), r"(\d+)\s*gb", 1).cast("float")
        df_hw = df_hw.withColumn(
            "storage_gb",
            when(storage_tb > 0, storage_tb).otherwise(storage_gb)
        ).withColumn(
            "storage_type",
            when(lower(col("raw_hard_drive_type")).contains("ssd"), lit("ssd"))
            .when(lower(col("raw_hard_drive_type")).contains("hdd"), lit("hdd"))
            .otherwise(lit("unknown"))
        )

        # ----------------------------------------------------------------------
        # 3. Đơn giản hóa NLP Quà tặng (bundles) thành các cờ nhị phân
        # ----------------------------------------------------------------------
        # Thay vì 500 chiều TF-IDF thưa thớt, trích xuất 4 cờ nhị phân thiết thực:
        # Chuột, Balo/Túi chống sốc, Sạc, và Có quà khác.
        bundle_lower = lower(coalesce(col("bundles"), lit("")))
        df_gifts = df_hw.withColumn(
            "gift_mouse",
            when(bundle_lower.rlike(r"chuột|mouse"), 1.0).otherwise(0.0)
        ).withColumn(
            "gift_bag",
            when(bundle_lower.rlike(r"balo|ba\s*lô|túi|cặp"), 1.0).otherwise(0.0)
        ).withColumn(
            "gift_charger",
            when(bundle_lower.rlike(r"sạc|cáp|adapter"), 1.0).otherwise(0.0)
        ).withColumn(
            "gift_other",
            when(col("bundles").isNotNull() & (trim(col("bundles")) != ""), 1.0).otherwise(0.0)
        )

        # ----------------------------------------------------------------------
        # 4. Xử lý Missing values theo đúng thực tế miền nghiệp vụ
        # ----------------------------------------------------------------------
        # - warranty_months: Laptop cũ không nhắc đến bảo hành thì 90% là hết hạn -> điền 0.0
        # - battery_cycles: Tạo cờ is_battery_cycle_known, tránh chế tác số chu kỳ sạc ảo cho Windows
        # - touch_sensitive: Missing xem như không có cảm ứng -> 0.0
        # - Loại bỏ các cột _src (Data Lineage không dùng trực tiếp làm feature huấn luyện)
        cols_to_drop = [c for c in df_gifts.columns if c.endswith("_src")]
        df_preprocessed = df_gifts.drop(*cols_to_drop)

        df_preprocessed = df_preprocessed.withColumn(
            "is_battery_cycle_known",
            when(col("battery_cycles").isNotNull(), 1.0).otherwise(0.0)
        ).fillna({
            "warranty_months": 0.0,
            "touch_sensitive": 0,
            "is_new_seal": 0,
            "is_like_new": 0,
            "is_upgraded": 0,
            "is_seller_rating_missing": 1,
            "seller_sold_count": 0,
        })

        # Điền missing cho các biến phân loại dạng text thành "unknown"
        categorical_cols = [
            "brand_name",
            "product_line",
            "cpu_vendor",
            "cpu_model",
            "gpu_type",
            "gpu_model",
            "storage_type",
            "resolution",
            "origin_type",
            "origin_country",
            "battery_condition",
            "warranty_type",
            "repair_status",
            "region_name",
        ]
        df_clean = df_preprocessed.fillna({c: "unknown" for c in categorical_cols})

        # Chuẩn hóa các cột nhị phân sang kiểu Float
        binary_cols = [
            "touch_sensitive",
            "is_new_seal",
            "is_like_new",
            "is_upgraded",
            "is_seller_rating_missing",
            "is_battery_cycle_known",
            "gift_mouse",
            "gift_bag",
            "gift_charger",
            "gift_other",
        ]
        for c in binary_cols:
            df_clean = df_clean.withColumn(c, col(c).cast("float"))

        # Chuẩn hóa các cột số học sang kiểu Float
        numeric_cols = [
            "ram_gb",
            "storage_gb",
            "screen_size_inch",
            "condition_pct",
            "battery_pct",
            "battery_cycles",
            "battery_hours",
            "warranty_months",
            "seller_rating",
            "seller_sold_count",
            "year_production",
        ]
        for c in numeric_cols:
            df_clean = df_clean.withColumn(c, col(c).cast("float"))

        # ----------------------------------------------------------------------
        # 5. Xây dựng ML Pipeline Transformers (Imputer, Indexer, OHE, Assembler)
        # ----------------------------------------------------------------------
        stages = []

        # 5.1. Imputer cho các giá trị số còn thiếu (Median strategy)
        imputer = Imputer(
            inputCols=numeric_cols,
            outputCols=[f"{c}_imputed" for c in numeric_cols]
        ).setStrategy("median")
        stages.append(imputer)

        # 5.2. StringIndexer + OneHotEncoder cho các biến phân loại
        indexer = StringIndexer(
            inputCols=categorical_cols,
            outputCols=[f"{c}_idx" for c in categorical_cols],
            handleInvalid="keep"
        )
        encoder = OneHotEncoder(
            inputCols=[f"{c}_idx" for c in categorical_cols],
            outputCols=[f"{c}_ohe" for c in categorical_cols],
            handleInvalid="keep"
        )
        stages.extend([indexer, encoder])

        # 5.3. VectorAssembler gom toàn bộ đặc trưng vào cột `features`
        # Sử dụng handleInvalid="keep" để không âm thầm drop bất kỳ dòng dữ liệu nào
        assembler_inputs = (
            binary_cols
            + [f"{c}_imputed" for c in numeric_cols]
            + [f"{c}_ohe" for c in categorical_cols]
        )
        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features",
            handleInvalid="keep"
        )
        stages.append(assembler)

        # ----------------------------------------------------------------------
        # 6. Fit Pipeline, Transform & Lưu Trữ Artifacts
        # ----------------------------------------------------------------------
        pipeline = Pipeline(stages=stages)
        pipeline_model = pipeline.fit(df_clean)
        df_gold = pipeline_model.transform(df_clean)

        df_gold_output = df_gold.select("post_id", "features", "target_price")

        logger.info(f"Saving Gold features Parquet to: {gold_dir}")
        os.makedirs(gold_dir, exist_ok=True)
        df_gold_output.write.format("parquet") \
            .option("compression", "snappy") \
            .mode("overwrite") \
            .save(gold_dir)

        logger.info(f"Saving PipelineModel artifact to: {model_artifact_dir}")
        os.makedirs(model_artifact_dir, exist_ok=True)
        pipeline_model.write().overwrite().save(model_artifact_dir)

        logger.info("Successfully finished Gold layer Feature Engineering pipeline.")
        return df_gold_output, pipeline_model

    except Exception as e:
        logger.error(f"Error in Gold layer Feature Engineering: {e}")
        raise
    finally:
        if is_owner:
            logger.info("Stopping SparkSession...")
            spark.stop()


if __name__ == "__main__":
    process_gold_layer()
