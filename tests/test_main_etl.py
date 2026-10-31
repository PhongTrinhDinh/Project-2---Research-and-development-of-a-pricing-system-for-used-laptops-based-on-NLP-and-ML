from unittest.mock import MagicMock, patch, call
import pytest

from src.data_pipeline.main_etl import run_etl_pipeline, create_orchestrator_spark_session


@pytest.fixture
def mock_spark_env():
    """Mock SparkContext để tránh assertion JVM khi chạy test."""
    from pyspark import SparkContext
    sc = MagicMock()
    sc._jvm.PythonUtils.getBroadcastThreshold.return_value = 1048576
    SparkContext._active_spark_context = sc
    SparkContext._jvm = sc._jvm
    return sc


def test_create_orchestrator_spark_session():
    """Kiểm tra create_orchestrator_spark_session cấu hình appName, connector MongoDB và nén Snappy."""
    with patch("src.data_pipeline.main_etl.SparkSession") as mock_spark_cls:
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_session = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session

        session = create_orchestrator_spark_session()

        assert session == mock_session
        mock_builder.appName.assert_called_once_with("Main_ETL_Orchestrator")
        mock_builder.config.assert_any_call("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2")
        mock_builder.config.assert_any_call("spark.sql.parquet.compression.codec", "snappy")
        mock_builder.getOrCreate.assert_called_once()


def test_run_etl_pipeline_success():
    """Kiểm tra toàn bộ quy trình pipeline (Bronze -> Silver -> Gold) dùng chung SparkSession, đo đếm thời gian và dòng."""
    mock_spark = MagicMock(name="SharedSparkSession")
    mock_df_bronze = MagicMock(name="DfBronze")
    mock_df_bronze.count.return_value = 1000

    mock_df_silver = MagicMock(name="DfSilver")
    mock_df_silver.count.return_value = 950

    mock_df_gold = MagicMock(name="DfGold")
    mock_df_gold.count.return_value = 920
    mock_model = MagicMock(name="PipelineModel")

    with (
        patch("src.data_pipeline.main_etl.create_orchestrator_spark_session", return_value=mock_spark) as p_create_spark,
        patch("src.data_pipeline.main_etl.ingest_bronze_layer", return_value=mock_df_bronze) as p_bronze,
        patch("src.data_pipeline.main_etl.process_silver_layer", return_value=mock_df_silver) as p_silver,
        patch("src.data_pipeline.main_etl.process_gold_layer", return_value=(mock_df_gold, mock_model)) as p_gold,
        patch("src.data_pipeline.main_etl.logger") as mock_logger,
    ):
        metrics = run_etl_pipeline()

        # 1. Kiểm tra tạo đúng 1 shared SparkSession và truyền xuống cả 3 tầng
        p_create_spark.assert_called_once()
        p_bronze.assert_called_once_with(spark=mock_spark)
        p_silver.assert_called_once_with(spark=mock_spark)
        p_gold.assert_called_once_with(spark=mock_spark)

        # 2. Kiểm tra tracking metrics
        assert metrics["status"] == "SUCCESS"
        assert metrics["bronze"]["row_count"] == 1000
        assert metrics["silver"]["row_count"] == 950
        assert metrics["gold"]["row_count"] == 920
        assert metrics["total_duration_sec"] >= 0.0

        # 3. Kiểm tra giải phóng shared SparkSession ở cuối
        mock_spark.stop.assert_called_once()


def test_run_etl_pipeline_with_external_spark():
    """Kiểm tra khi truyền spark từ ngoài vào, main_etl không tự ý stop session của bên ngoài."""
    external_spark = MagicMock(name="ExternalSparkSession")
    mock_df_bronze = MagicMock()
    mock_df_bronze.count.return_value = 100
    mock_df_silver = MagicMock()
    mock_df_silver.count.return_value = 90
    mock_df_gold = MagicMock()
    mock_df_gold.count.return_value = 85

    with (
        patch("src.data_pipeline.main_etl.create_orchestrator_spark_session") as p_create_spark,
        patch("src.data_pipeline.main_etl.ingest_bronze_layer", return_value=mock_df_bronze),
        patch("src.data_pipeline.main_etl.process_silver_layer", return_value=mock_df_silver),
        patch("src.data_pipeline.main_etl.process_gold_layer", return_value=(mock_df_gold, MagicMock())),
    ):
        metrics = run_etl_pipeline(spark=external_spark)

        # Không tự tạo session mới
        p_create_spark.assert_not_called()
        assert metrics["status"] == "SUCCESS"

        # Không stop external session
        external_spark.stop.assert_not_called()


def test_run_etl_pipeline_empty_bronze_gate():
    """Kiểm tra Data Quality Gate: Nếu Bronze nạp 0 dòng, pipeline dừng an toàn và không gọi Silver / Gold."""
    mock_spark = MagicMock(name="SharedSparkSession")
    mock_df_empty = MagicMock()
    mock_df_empty.count.return_value = 0

    with (
        patch("src.data_pipeline.main_etl.create_orchestrator_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.main_etl.ingest_bronze_layer", return_value=mock_df_empty) as p_bronze,
        patch("src.data_pipeline.main_etl.process_silver_layer") as p_silver,
        patch("src.data_pipeline.main_etl.process_gold_layer") as p_gold,
        patch("src.data_pipeline.main_etl.logger") as mock_logger,
    ):
        metrics = run_etl_pipeline()

        assert metrics["status"] == "HALTED_EMPTY_BRONZE"
        p_bronze.assert_called_once_with(spark=mock_spark)
        p_silver.assert_not_called()
        p_gold.assert_not_called()

        mock_logger.warning.assert_called_once()
        assert "Bronze layer produced 0 records" in mock_logger.warning.call_args[0][0]

        # Đảm bảo SparkSession vẫn được giải phóng
        mock_spark.stop.assert_called_once()


def test_run_etl_pipeline_empty_silver_gate():
    """Kiểm tra Data Quality Gate: Nếu Silver sau làm sạch còn 0 dòng, pipeline dừng an toàn và không gọi Gold."""
    mock_spark = MagicMock(name="SharedSparkSession")
    mock_df_bronze = MagicMock()
    mock_df_bronze.count.return_value = 100
    mock_df_silver_empty = MagicMock()
    mock_df_silver_empty.count.return_value = 0

    with (
        patch("src.data_pipeline.main_etl.create_orchestrator_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.main_etl.ingest_bronze_layer", return_value=mock_df_bronze),
        patch("src.data_pipeline.main_etl.process_silver_layer", return_value=mock_df_silver_empty),
        patch("src.data_pipeline.main_etl.process_gold_layer") as p_gold,
        patch("src.data_pipeline.main_etl.logger") as mock_logger,
    ):
        metrics = run_etl_pipeline()

        assert metrics["status"] == "HALTED_EMPTY_SILVER"
        p_gold.assert_not_called()

        mock_logger.warning.assert_called_once()
        assert "Silver layer produced 0 records" in mock_logger.warning.call_args[0][0]

        mock_spark.stop.assert_called_once()


def test_run_etl_pipeline_failure_cleanup():
    """Kiểm tra khi xảy ra lỗi ở bất kỳ tầng nào, lỗi được bắt, log ghi nhận và SparkSession luôn được đóng."""
    mock_spark = MagicMock(name="SharedSparkSession")

    with (
        patch("src.data_pipeline.main_etl.create_orchestrator_spark_session", return_value=mock_spark),
        patch("src.data_pipeline.main_etl.ingest_bronze_layer", side_effect=ConnectionError("MongoDB Atlas unreachable")),
        patch("src.data_pipeline.main_etl.logger") as mock_logger,
    ):
        with pytest.raises(ConnectionError, match="MongoDB Atlas unreachable"):
            run_etl_pipeline()

        mock_logger.error.assert_called_once()
        assert "Pipeline failed critically" in mock_logger.error.call_args[0][0]

        # Kiểm tra finally đóng SparkSession
        mock_spark.stop.assert_called_once()


def test_main_entrypoint():
    """Kiểm tra hàm main() khởi chạy run_etl_pipeline an toàn."""
    with patch("src.data_pipeline.main_etl.run_etl_pipeline") as mock_run:
        from src.data_pipeline.main_etl import main
        main()
        mock_run.assert_called_once()
