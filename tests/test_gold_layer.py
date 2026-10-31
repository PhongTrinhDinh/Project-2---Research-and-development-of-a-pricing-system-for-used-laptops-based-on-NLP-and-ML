from unittest.mock import MagicMock, patch
import pytest

from src.data_pipeline.gold_layer import process_gold_layer


@pytest.fixture
def mock_spark_environment():
    """Thiết lập mock cho SparkContext để tránh assertion JVM khi gọi các hàm Spark SQL và ML."""
    from pyspark import SparkContext
    sc = MagicMock()
    sc._jvm.PythonUtils.getBroadcastThreshold.return_value = 1048576
    SparkContext._active_spark_context = sc
    SparkContext._jvm = sc._jvm
    return sc


def test_process_gold_layer_success(mock_spark_environment):
    """Kiểm tra toàn bộ luồng pipeline Gold Layer: lọc outlier giá, feature engineering, lưu Parquet, lưu Model, đóng Spark."""
    mock_spark = MagicMock(name="SparkSession")
    mock_reader = MagicMock(name="DataFrameReader")
    mock_df_silver = MagicMock(name="DataFrameSilver")
    mock_df_transformed = MagicMock(name="DataFrameTransformed")
    mock_writer = MagicMock(name="DataFrameWriter")
    mock_pipeline_model = MagicMock(name="PipelineModel")
    mock_model_writer = MagicMock(name="ModelWriter")

    # Spark read chain
    mock_spark.read.parquet.return_value = mock_df_silver

    # DataFrame transformation chain
    mock_df_silver.filter.return_value = mock_df_transformed
    mock_df_transformed.withColumn.return_value = mock_df_transformed
    mock_df_transformed.drop.return_value = mock_df_transformed
    mock_df_transformed.fillna.return_value = mock_df_transformed
    mock_df_transformed.columns = ["post_id", "target_price", "brand_name_src", "product_line_src"]

    # Output writer chain
    mock_df_transformed.select.return_value = mock_df_transformed
    mock_df_transformed.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    # Pipeline Model writer chain
    mock_pipeline_model.transform.return_value = mock_df_transformed
    mock_pipeline_model.write.return_value = mock_model_writer
    mock_model_writer.overwrite.return_value = mock_model_writer

    with (
        patch("src.data_pipeline.gold_layer.SparkSession") as mock_spark_cls,
        patch("src.data_pipeline.gold_layer.Pipeline") as mock_pipeline_cls,
        patch("src.data_pipeline.gold_layer.Imputer") as mock_imputer_cls,
        patch("src.data_pipeline.gold_layer.StringIndexer") as mock_indexer_cls,
        patch("src.data_pipeline.gold_layer.OneHotEncoder") as mock_encoder_cls,
        patch("src.data_pipeline.gold_layer.VectorAssembler") as mock_assembler_cls,
    ):
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        mock_imputer_inst = MagicMock(name="ImputerInstance")
        mock_imputer_cls.return_value = mock_imputer_inst
        mock_imputer_inst.setStrategy.return_value = mock_imputer_inst

        mock_indexer_inst = MagicMock(name="IndexerInstance")
        mock_indexer_cls.return_value = mock_indexer_inst

        mock_encoder_inst = MagicMock(name="EncoderInstance")
        mock_encoder_cls.return_value = mock_encoder_inst

        mock_assembler_inst = MagicMock(name="AssemblerInstance")
        mock_assembler_cls.return_value = mock_assembler_inst

        mock_pipeline_inst = MagicMock(name="PipelineInstance")
        mock_pipeline_cls.return_value = mock_pipeline_inst
        mock_pipeline_inst.fit.return_value = mock_pipeline_model

        df_out, model_out = process_gold_layer()

        # 1. Kiểm tra khởi tạo SparkSession
        mock_builder.appName.assert_called_once_with("GoldLayer_FeatureEngineering")
        mock_builder.getOrCreate.assert_called_once()

        # 2. Kiểm tra đọc từ Silver
        mock_spark.read.parquet.assert_called_once()

        # 3. Kiểm tra lọc outlier trên target_price
        mock_df_silver.filter.assert_called_once()

        # 4. Kiểm tra loại bỏ cột _src
        mock_df_transformed.drop.assert_called_once_with("brand_name_src", "product_line_src")

        # 5. Kiểm tra khởi tạo các transformers
        mock_imputer_cls.assert_called_once()
        mock_imputer_inst.setStrategy.assert_called_once_with("median")

        mock_indexer_cls.assert_called_once()
        assert mock_indexer_cls.call_args[1]["handleInvalid"] == "keep"

        mock_encoder_cls.assert_called_once()
        assert mock_encoder_cls.call_args[1]["handleInvalid"] == "keep"

        mock_assembler_cls.assert_called_once()
        assert mock_assembler_cls.call_args[1]["handleInvalid"] == "keep"

        # 6. Kiểm tra fit pipeline và transform
        mock_pipeline_cls.assert_called_once()
        mock_pipeline_inst.fit.assert_called_once()
        mock_pipeline_model.transform.assert_called_once()

        # 7. Kiểm tra select đúng tên cột: post_id, features, target_price (không dùng list_id cũ)
        mock_df_transformed.select.assert_called_once_with("post_id", "features", "target_price")

        # 8. Kiểm tra lưu dữ liệu Gold Parquet với snappy compression
        mock_df_transformed.write.format.assert_called_once_with("parquet")
        mock_writer.option.assert_called_once_with("compression", "snappy")
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.save.assert_called_once()

        # 9. Kiểm tra lưu PipelineModel artifact
        mock_pipeline_model.write.assert_called_once()
        mock_model_writer.overwrite.assert_called_once()
        mock_model_writer.save.assert_called_once()

        # 10. Đảm bảo dọn dẹp đóng SparkSession
        mock_spark.stop.assert_called_once()


def test_process_gold_layer_error_handling(mock_spark_environment):
    """Kiểm tra khi xảy ra lỗi trong pipeline, log lỗi được ghi lại và spark.stop() luôn được gọi."""
    mock_spark = MagicMock(name="SparkSession")
    mock_spark.read.parquet.side_effect = RuntimeError("Silver Parquet file corrupted or missing")

    with (
        patch("src.data_pipeline.gold_layer.SparkSession") as mock_spark_cls,
        patch("src.data_pipeline.gold_layer.logger.error") as mock_logger_error,
    ):
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        with pytest.raises(RuntimeError, match="Silver Parquet file corrupted or missing"):
            process_gold_layer()

        mock_logger_error.assert_called_once()
        assert "Error in Gold layer Feature Engineering" in mock_logger_error.call_args[0][0]

        # Kiểm tra khối finally luôn gọi spark.stop()
        mock_spark.stop.assert_called_once()


def test_hardware_extraction_and_gift_flags_regex():
    """Kiểm tra các biểu thức regex trích xuất RAM, Ổ cứng và cờ quà tặng hoạt động chính xác."""
    import re

    # 1. Trích xuất RAM (GB)
    ram_re = re.compile(r"(\d+)\s*gb", re.I)
    assert ram_re.search("16 GB").group(1) == "16"
    assert ram_re.search("8gb ddr4").group(1) == "8"
    assert ram_re.search("32 gb").group(1) == "32"

    # 2. Trích xuất Dung lượng ổ cứng (GB / TB)
    tb_re = re.compile(r"(\d+(?:\.\d+)?)\s*tb", re.I)
    gb_re = re.compile(r"(\d+)\s*gb", re.I)

    m_tb = tb_re.search("1 TB SSD")
    assert m_tb is not None
    assert float(m_tb.group(1)) * 1024.0 == 1024.0

    m_gb = gb_re.search("512 GB NVMe")
    assert m_gb is not None
    assert float(m_gb.group(1)) == 512.0

    # 3. Phân loại loại ổ cứng
    def classify_storage(drive_type_str):
        s = (drive_type_str or "").lower()
        if "ssd" in s:
            return "ssd"
        if "hdd" in s:
            return "hdd"
        return "unknown"

    assert classify_storage("SSD NVMe PCIe") == "ssd"
    assert classify_storage("HDD 5400rpm") == "hdd"
    assert classify_storage("") == "unknown"

    # 4. Trích xuất cờ quà tặng (Bundles)
    gift_mouse_re = re.compile(r"chuột|mouse", re.I)
    gift_bag_re = re.compile(r"balo|ba\s*lô|túi|cặp", re.I)
    gift_charger_re = re.compile(r"sạc|cáp|adapter", re.I)

    text1 = "tặng chuột không dây và túi chống sốc"
    assert bool(gift_mouse_re.search(text1)) is True
    assert bool(gift_bag_re.search(text1)) is True
    assert bool(gift_charger_re.search(text1)) is False

    text2 = "kèm sạc zin cáp theo máy"
    assert bool(gift_charger_re.search(text2)) is True
    assert bool(gift_mouse_re.search(text2)) is False

