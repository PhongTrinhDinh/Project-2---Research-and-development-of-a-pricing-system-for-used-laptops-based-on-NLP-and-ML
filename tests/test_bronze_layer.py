from unittest.mock import MagicMock, patch
import pytest

from src.data_pipeline.bronze_layer import create_spark_session, ingest_bronze_layer


@pytest.fixture
def mock_env(monkeypatch):
    """Fixture thiết lập các biến môi trường phục vụ kiểm thử."""
    env_vars = {
        "MONGO_URI": "mongodb://localhost:27017",
        "MONGO_DB": "chotot_db",
        "MONGO_COLL_RAW": "raw_laptops",
        "DATALAKE_BRONZE_DIR": "/datalake/bronze/laptops",
    }
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)
    return env_vars


@pytest.fixture
def mock_spark_pipeline():
    """Fixture tạo cây mock hoàn chỉnh cho các đối tượng của PySpark trong pipeline."""
    mock_spark = MagicMock(name="SparkSession")
    mock_reader = MagicMock(name="DataFrameReader")
    mock_df_raw = MagicMock(name="DataFrameRaw")
    mock_df_transformed = MagicMock(name="DataFrameBronze")
    mock_writer = MagicMock(name="DataFrameWriter")

    # spark.read chain
    mock_spark.read.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.return_value = mock_df_raw

    # df_raw count & withColumn chain
    mock_df_raw.count.return_value = 150
    mock_df_raw.withColumn.return_value = mock_df_transformed
    mock_df_transformed.withColumn.return_value = mock_df_transformed

    # df_bronze.write chain
    mock_df_transformed.write.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.partitionBy.return_value = mock_writer

    return {
        "spark": mock_spark,
        "reader": mock_reader,
        "df_raw": mock_df_raw,
        "df_transformed": mock_df_transformed,
        "writer": mock_writer,
    }


@pytest.fixture
def mock_spark_functions():
    """Fixture mock các hàm current_timestamp, to_date, lit của pyspark.sql.functions."""
    with (
        patch("src.data_pipeline.bronze_layer.current_timestamp") as mock_ts,
        patch("src.data_pipeline.bronze_layer.to_date") as mock_to_date,
        patch("src.data_pipeline.bronze_layer.lit") as mock_lit,
    ):
        mock_ts.return_value = MagicMock(name="mock_current_timestamp")
        mock_to_date.return_value = MagicMock(name="mock_to_date")
        mock_lit.return_value = MagicMock(name="mock_lit")
        yield {
            "current_timestamp": mock_ts,
            "to_date": mock_to_date,
            "lit": mock_lit,
        }


def test_create_spark_session():
    """Kiểm tra create_spark_session cấu hình đúng appName và mongo connector package."""
    with patch("src.data_pipeline.bronze_layer.SparkSession") as mock_spark_cls:
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_session = MagicMock()
        mock_builder.getOrCreate.return_value = mock_session

        session = create_spark_session()

        assert session == mock_session
        mock_builder.appName.assert_called_once_with("BronzeLayer_MongoDB_Ingestion")
        mock_builder.config.assert_called_once_with(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2",
        )
        mock_builder.getOrCreate.assert_called_once()


def test_ingest_bronze_layer_success(mock_env, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra pipeline ingest_bronze_layer chạy thành công toàn bộ luồng (Read -> Transform -> Write -> Stop)."""
    mock_spark = mock_spark_pipeline["spark"]
    mock_reader = mock_spark_pipeline["reader"]
    mock_df_raw = mock_spark_pipeline["df_raw"]
    mock_df_transformed = mock_spark_pipeline["df_transformed"]
    mock_writer = mock_spark_pipeline["writer"]

    with patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark):
        ingest_bronze_layer()

    # 1. Kiểm tra đọc từ MongoDB đúng format, URI và sampleSize
    mock_spark.read.format.assert_called_once_with("mongo")
    expected_uri = f"{mock_env['MONGO_URI']}/{mock_env['MONGO_DB']}.{mock_env['MONGO_COLL_RAW']}"
    mock_reader.option.assert_any_call("uri", expected_uri)
    mock_reader.option.assert_any_call("sampleSize", 50000)
    mock_reader.load.assert_called_once()
    mock_df_raw.count.assert_called_once()

    # 2. Kiểm tra gọi các hàm PySpark functions đúng cách
    assert mock_spark_functions["current_timestamp"].call_count == 2
    mock_spark_functions["to_date"].assert_called_once_with(
        mock_spark_functions["current_timestamp"].return_value
    )
    mock_spark_functions["lit"].assert_called_once_with("mongo_scrapy")

    # 3. Kiểm tra các bước thêm metadata columns
    first_col_call = mock_df_raw.withColumn.call_args
    assert first_col_call[0][0] == "_ingested_at"
    assert first_col_call[0][1] == mock_spark_functions["current_timestamp"].return_value

    second_col_call = mock_df_transformed.withColumn.call_args_list[0]
    assert second_col_call[0][0] == "_ingested_date"
    assert second_col_call[0][1] == mock_spark_functions["to_date"].return_value

    third_col_call = mock_df_transformed.withColumn.call_args_list[1]
    assert third_col_call[0][0] == "_source"
    assert third_col_call[0][1] == mock_spark_functions["lit"].return_value

    # 4. Kiểm tra ghi dữ liệu Parquet với partitionBy và mode append
    mock_df_transformed.write.format.assert_called_once_with("parquet")
    mock_writer.mode.assert_called_once_with("append")
    mock_writer.partitionBy.assert_called_once_with("_ingested_date")
    mock_writer.save.assert_called_once_with(mock_env["DATALAKE_BRONZE_DIR"])

    # 5. Kiểm tra luôn dọn dẹp và đóng SparkSession
    mock_spark.stop.assert_called_once()


def test_ingest_bronze_layer_read_failure(mock_env, mock_spark_pipeline):
    """Kiểm tra xử lý lỗi khi đọc từ MongoDB thất bại: log lỗi, ném ngoại lệ và vẫn gọi spark.stop()."""
    mock_spark = mock_spark_pipeline["spark"]
    mock_reader = mock_spark_pipeline["reader"]
    mock_reader.load.side_effect = ConnectionError("Could not connect to MongoDB server")

    with patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark):
        with patch("src.data_pipeline.bronze_layer.logger.error") as mock_logger_error:
            with pytest.raises(ConnectionError, match="Could not connect to MongoDB server"):
                ingest_bronze_layer()

            mock_logger_error.assert_called_once()
            assert "Error in ETL Bronze" in mock_logger_error.call_args[0][0]

    # Đảm bảo khối finally luôn gọi spark.stop() dù xảy ra lỗi
    mock_spark.stop.assert_called_once()


def test_ingest_bronze_layer_write_failure(mock_env, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra xử lý lỗi khi ghi dữ liệu ra Datalake thất bại: log lỗi, ném ngoại lệ và vẫn gọi spark.stop()."""
    mock_spark = mock_spark_pipeline["spark"]
    mock_writer = mock_spark_pipeline["writer"]
    mock_writer.save.side_effect = PermissionError("Permission denied writing to Datalake")

    with patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark):
        with patch("src.data_pipeline.bronze_layer.logger.error") as mock_logger_error:
            with pytest.raises(PermissionError, match="Permission denied writing to Datalake"):
                ingest_bronze_layer()

            mock_logger_error.assert_called_once()
            assert "Error in ETL Bronze" in mock_logger_error.call_args[0][0]

    # Đảm bảo khối finally luôn gọi spark.stop() dù lỗi ở bước ghi
    mock_spark.stop.assert_called_once()


def test_ingest_bronze_layer_custom_env_paths(monkeypatch, mock_spark_pipeline, mock_spark_functions):
    """Kiểm tra pipeline xây dựng đúng URI và output path với các giá trị môi trường tùy chỉnh khác nhau."""
    custom_env = {
        "MONGO_URI": "mongodb+srv://admin:pass@cluster.mongodb.net",
        "MONGO_DB": "prod_warehouse",
        "MONGO_COLL_RAW": "chotot_crawler_v2",
        "DATALAKE_BRONZE_DIR": "s3a://my-lake-bucket/bronze/laptops",
    }
    for k, v in custom_env.items():
        monkeypatch.setenv(k, v)

    mock_spark = mock_spark_pipeline["spark"]
    mock_reader = mock_spark_pipeline["reader"]
    mock_writer = mock_spark_pipeline["writer"]

    with patch("src.data_pipeline.bronze_layer.create_spark_session", return_value=mock_spark):
        ingest_bronze_layer()

    expected_full_uri = "mongodb+srv://admin:pass@cluster.mongodb.net/prod_warehouse.chotot_crawler_v2"
    mock_reader.option.assert_any_call("uri", expected_full_uri)
    mock_writer.save.assert_called_once_with("s3a://my-lake-bucket/bronze/laptops")
    mock_spark.stop.assert_called_once()
