import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.utils.logger import get_logger
from src.data_pipeline.bronze_layer import ingest_bronze_layer
from src.data_pipeline.silver_layer import process_silver_layer
from src.data_pipeline.gold_layer import process_gold_layer

# Tải cấu hình môi trường (.env) ngay từ đầu để đảm bảo các tầng con có đủ biến cấu hình
load_dotenv()

# Thiết lập logger riêng có ghi file log tập trung cho Orchestrator
logger = get_logger("ETL_Orchestrator", log_file="logs/etl_pipeline/main_etl.log")


def create_orchestrator_spark_session() -> SparkSession:
    """
    Khởi tạo SparkSession dùng chung cho toàn bộ pipeline ETL.
    Cấu hình sẵn connector MongoDB và chuẩn nén Parquet Snappy.
    """
    mongo_connector_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    return (
        SparkSession.builder.appName("Main_ETL_Orchestrator")
        .config("spark.jars.packages", mongo_connector_pkg)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def run_etl_pipeline(spark: SparkSession = None) -> dict:
    """
    Điều phối toàn bộ quy trình ETL Medallion: Bronze -> Silver -> Gold.

    Cơ chế tối ưu:
    - Sử dụng chung 1 SparkSession xuyên suốt 3 tầng (tránh bật/tắt JVM 3 lần).
    - Đo đếm thời gian (seconds) và đếm số dòng (row counts) từng tầng.
    - Data Quality Gate: Dừng an toàn nếu phát hiện 0 dòng từ tầng trước.
    - Bắt lỗi, ghi log chi tiết và tự động giải phóng SparkSession.
    """
    total_start = time.time()
    logger.info("==================================================================")
    logger.info("START RUNNING MEDALLION ETL PIPELINE (Bronze -> Silver -> Gold)")
    logger.info("==================================================================")

    is_owner = False
    if spark is None:
        logger.info("Initializing shared SparkSession for ETL pipeline...")
        spark = create_orchestrator_spark_session()
        is_owner = True

    metrics = {
        "status": "FAILED",
        "bronze": {"duration_sec": 0.0, "row_count": 0},
        "silver": {"duration_sec": 0.0, "row_count": 0},
        "gold": {"duration_sec": 0.0, "row_count": 0},
        "total_duration_sec": 0.0,
    }

    try:
        # ----------------------------------------------------------------------
        # [1/3] Bronze Layer: Ingest MongoDB raw data -> Raw Parquet
        # ----------------------------------------------------------------------
        t0 = time.time()
        logger.info("[1/3] Activating Bronze layer...")
        df_bronze = ingest_bronze_layer(spark=spark)
        b_time = round(time.time() - t0, 2)
        b_count = df_bronze.count() if df_bronze is not None else 0
        metrics["bronze"] = {"duration_sec": b_time, "row_count": b_count}
        logger.info(f"[1/3] Bronze layer completed: {b_count:,} records in {b_time}s")

        # Data Quality Gate: Dừng nếu không có dữ liệu từ MongoDB
        if b_count == 0:
            logger.warning(
                "Bronze layer produced 0 records. Halting pipeline to prevent empty downstream processing."
            )
            metrics["status"] = "HALTED_EMPTY_BRONZE"
            metrics["total_duration_sec"] = round(time.time() - total_start, 2)
            return metrics

        # ----------------------------------------------------------------------
        # [2/3] Silver Layer: Deduplication, Cleaning, Feature Extraction
        # ----------------------------------------------------------------------
        t0 = time.time()
        logger.info("[2/3] Activating Silver layer...")
        df_silver = process_silver_layer(spark=spark)
        s_time = round(time.time() - t0, 2)
        s_count = df_silver.count() if df_silver is not None else 0
        metrics["silver"] = {"duration_sec": s_time, "row_count": s_count}
        logger.info(f"[2/3] Silver layer completed: {s_count:,} valid unique records in {s_time}s")

        # Data Quality Gate: Dừng nếu không còn bản ghi hợp lệ sau Silver
        if s_count == 0:
            logger.warning(
                "Silver layer produced 0 records after cleaning. Halting pipeline to avoid Gold failure."
            )
            metrics["status"] = "HALTED_EMPTY_SILVER"
            metrics["total_duration_sec"] = round(time.time() - total_start, 2)
            return metrics

        # ----------------------------------------------------------------------
        # [3/3] Gold Layer: Outlier Filtering, Encoding, VectorAssembler
        # ----------------------------------------------------------------------
        t0 = time.time()
        logger.info("[3/3] Activating Gold layer...")
        df_gold, pipeline_model = process_gold_layer(spark=spark)
        g_time = round(time.time() - t0, 2)
        g_count = df_gold.count() if df_gold is not None else 0
        metrics["gold"] = {"duration_sec": g_time, "row_count": g_count}
        logger.info(f"[3/3] Gold layer completed: {g_count:,} feature vectors in {g_time}s")

        # Hoàn tất thành công
        total_time = round(time.time() - total_start, 2)
        metrics["total_duration_sec"] = total_time
        metrics["status"] = "SUCCESS"

        logger.info("==================================================================")
        logger.info(f"COMPLETE THE ENTIRE ETL PIPELINE IN {total_time}s!")
        logger.info(
            f"Execution Summary: Bronze={b_count:,} rows ({b_time}s) -> "
            f"Silver={s_count:,} rows ({s_time}s) -> Gold={g_count:,} rows ({g_time}s)"
        )
        logger.info("==================================================================")
        return metrics

    except Exception as e:
        total_time = round(time.time() - total_start, 2)
        metrics["total_duration_sec"] = total_time
        metrics["status"] = "FAILED"
        logger.error(f"Pipeline failed critically after {total_time}s. Error: {e}")
        raise
    finally:
        if is_owner and spark is not None:
            logger.info("Shutting down shared SparkSession...")
            spark.stop()


def main():
    """Hàm main entry point để chạy toàn bộ pipeline."""
    run_etl_pipeline()


if __name__ == "__main__":
    main()
