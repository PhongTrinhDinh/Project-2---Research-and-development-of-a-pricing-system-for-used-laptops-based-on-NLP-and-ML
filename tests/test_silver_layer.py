import re
from unittest.mock import MagicMock, patch
import pytest

from src.data_pipeline import silver_patterns
from src.data_pipeline.silver_layer import process_silver_layer


def to_python_re(pattern: str) -> re.Pattern:
    """
    Chuyển đổi các lớp ký tự Unicode của Java regex (\\p{L}\\p{N}) sang định dạng
    tương đương của Python standard re ([^\\W_]) để phục vụ kiểm thử đơn vị độc lập.
    """
    p = pattern.replace(r"\p{L}\p{N}", r"^\W_")
    return re.compile(p, re.IGNORECASE | re.UNICODE)


# ==============================================================================
# 1. Pipeline Structure & ML Logic Tests (Using PySpark Mocks)
# ==============================================================================
def test_process_silver_layer_pipeline():
    """Kiểm tra toàn bộ kiến trúc pipeline Silver layer: đọc Bronze, missing values, ghi Parquet, đóng Spark."""
    from pyspark import SparkContext
    sc = MagicMock()
    sc._jvm.PythonUtils.getBroadcastThreshold.return_value = 1048576
    SparkContext._active_spark_context = sc

    mock_spark = MagicMock(name="SparkSession")
    mock_reader = MagicMock(name="DataFrameReader")
    mock_df_bronze = MagicMock(name="DataFrameBronze")
    mock_df_transformed = MagicMock(name="DataFrameSilver")
    mock_writer = MagicMock(name="DataFrameWriter")

    mock_spark.read.parquet.return_value = mock_df_bronze
    mock_df_bronze.dropDuplicates.return_value = mock_df_bronze
    mock_df_bronze.select.return_value = mock_df_transformed
    mock_df_transformed.withColumn.return_value = mock_df_transformed
    mock_df_transformed.fillna.return_value = mock_df_transformed
    mock_df_transformed.drop.return_value = mock_df_transformed
    mock_df_transformed.write.format.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer

    with patch("src.data_pipeline.silver_layer.SparkSession") as mock_spark_cls:
        mock_builder = MagicMock()
        mock_spark_cls.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        res = process_silver_layer()

        # Kiểm tra cấu hình SparkSession
        mock_builder.appName.assert_called_once_with("SilverLayer_Processing")
        mock_builder.getOrCreate.assert_called_once()

        # Kiểm tra đọc từ bronze
        mock_spark.read.parquet.assert_called_once()

        # Kiểm tra cấu hình ghi Parquet snappy overwrite
        mock_df_transformed.write.format.assert_called_once_with("parquet")
        mock_writer.option.assert_called_once_with("compression", "snappy")
        mock_writer.mode.assert_called_once_with("overwrite")
        mock_writer.save.assert_called_once()

        # Kiểm tra đóng SparkSession
        mock_spark.stop.assert_called_once()



# ==============================================================================
# 2. Regex Edge Cases & Bug Fix Verification Tests
# ==============================================================================

def test_cpu_model_regex_fixes():
    """Kiểm tra các lỗi trích xuất CPU: đuôi thế hệ 11 (1135g7), chống dương tính dung lượng (512gb), M2 NVMe, Ryzen Pro."""
    intel_re = to_python_re(silver_patterns.INTEL_MODEL_PATTERN)
    apple_re = to_python_re(silver_patterns.APPLE_M_PATTERN)
    amd_re = to_python_re(silver_patterns.AMD_MODEL_PATTERN)

    # 1. Cắt cụt đuôi thế hệ 11: i5 1135g7 phải bắt trọn vẹn cả g7
    m = intel_re.search("laptop dell i5 1135g7 ram 8gb")
    assert m is not None
    assert m.group(1).replace(" ", "") == "i51135g7"

    # 2. Dương tính giả với dung lượng: i5 512gb không được bắt thành i5 512
    # Trong chuỗi này, 512 đi kèm gb nên không phải là mã CPU hợp lệ
    m_storage = intel_re.search("laptop dell core i5 512gb ssd")
    # i5 đứng riêng không có mã đuôi, hoặc không bắt nhầm 512gb
    if m_storage:
        assert "512" not in m_storage.group(1)

    # 3. m2 nvme (SSD) không được bắt thành chip Apple M2
    m_nvme = apple_re.search("máy dell gắn ssd m2 nvme 256gb")
    assert m_nvme is None

    # M2 Pro chuẩn trên Macbook thì phải bắt được
    m_apple = apple_re.search("bán macbook pro m2 pro 16gb")
    assert m_apple is not None
    assert "m2 pro" in m_apple.group(1).lower()

    # 4. AMD Ryzen giữ lại pro và hậu tố hs
    m_amd_pro = amd_re.search("thinkpad chạy ryzen 7 pro 4750u")
    assert m_amd_pro is not None
    assert "pro 4750u" in m_amd_pro.group(1).lower()

    m_amd_hs = amd_re.search("asus rog ryzen 5 5600hs rtx 3050")
    assert m_amd_hs is not None
    assert "5600hs" in m_amd_hs.group(1).lower()


def test_line_model_and_brand_regex():
    """Kiểm tra trích xuất dòng máy không khoảng trắng (xps13), mã có chữ (latitude e7470, thinkpad x1 carbon, elitebook 840 g5)."""
    line_re = to_python_re(silver_patterns.PRODUCT_LINE_PATTERN)

    assert line_re.search("bán dell xps13 9300") is not None
    assert line_re.search("dell latitude e7470 bền bỉ") is not None
    assert line_re.search("lenovo thinkpad x1 carbon gen 7") is not None
    assert line_re.search("hp elitebook 840 g5 i5") is not None
    assert line_re.search("laptop asus vivobook 15 oled") is not None
    assert line_re.search("lenovo legion 5 pro 2022") is not None
    assert line_re.search("acer nitro 5 gaming") is not None


def test_gpu_regex_fixes():
    """Kiểm tra nhận diện GPU 3 chữ số cũ (gtx 960m), mx250 và phân biệt card rời / card tích hợp."""
    discrete_re = to_python_re(silver_patterns.GPU_DISCRETE_PATTERN)
    integrated_re = to_python_re(silver_patterns.GPU_INTEGRATED_PATTERN)

    # Card rời 3 chữ số
    m_960 = discrete_re.search("máy chơi game gtx 960m mượt")
    assert m_960 is not None
    assert "960m" in m_960.group(1).lower()

    # Card rời MX
    m_mx = discrete_re.search("laptop văn phòng mx250 2gb")
    assert m_mx is not None
    assert "mx250" in m_mx.group(1).lower()

    # Card tích hợp iris xe, uhd
    assert integrated_re.search("intel iris xe graphics") is not None
    assert integrated_re.search("intel uhd graphics 620") is not None

    # Radeon đứng một mình đi cùng chip tích hợp
    assert integrated_re.search("amd ryzen 5 with radeon graphics") is not None


def test_screen_size_comma_and_unitless():
    """Kiểm tra màn hình nhận dấu phẩy (15,6 inch) và không có đơn vị (laptop 15.6 fhd)."""
    screen_re = to_python_re(silver_patterns.SCREEN_SIZE_PATTERN)

    m1 = screen_re.search("màn hình 15,6 inch fhd")
    assert m1 is not None
    assert m1.group(1).replace(",", ".") == "15.6"

    m2 = screen_re.search("laptop 15.6 fhd ips")
    assert m2 is not None
    assert m2.group(1) == "15.6"

    m3 = screen_re.search("dell latitude 14 inch")
    assert m3 is not None
    assert m3.group(1) == "14"


def test_resolution_regex():
    """Kiểm tra độ phân giải dạng số 1920x1200 và tránh dương tính giả 3k giá tiền."""
    num_re = to_python_re(silver_patterns.RESOLUTION_NUM_PATTERN)
    text_re = to_python_re(silver_patterns.RESOLUTION_TEXT_PATTERN)

    # 1920x1200 (16:10 FHD+)
    m_num = num_re.search("màn 1920x1200 sáng đẹp")
    assert m_num is not None
    assert m_num.group(1) == "1920"
    assert m_num.group(2) == "1200"

    # Tránh nhầm "bán 3k" (3 nghìn đồng)
    assert text_re.search("bán rẻ 3k máy tốt") is None
    # Nhưng trong ngữ cảnh màn hình thì bắt được
    assert text_re.search("màn hình 3k oled") is not None
    assert text_re.search("độ phân giải 2k 120hz") is not None


def test_touch_sensitive_word_boundary():
    """Kiểm tra touch_sensitive không bị dương tính giả với touchpad và touch bar."""
    pos_re = to_python_re(silver_patterns.TOUCH_POS_PATTERN)
    neg_re = to_python_re(silver_patterns.TOUCH_NEG_PATTERN)

    # Touchpad và Touch Bar không được coi là màn hình cảm ứng
    assert pos_re.search("touchpad rộng rãi mượt mà") is None
    assert pos_re.search("macbook có touch bar xịn") is None
    assert pos_re.search("bảo mật touch id nhạy") is None

    # Màn cảm ứng thật
    assert pos_re.search("màn hình cảm ứng đa điểm") is not None
    assert pos_re.search("xoay gập touch screen tiện lợi") is not None

    # Phủ định
    assert neg_re.search("bản màn không cảm ứng") is not None
    assert neg_re.search("phiên bản non-touch") is not None


def test_origin_word_boundary_fixes():
    """Kiểm tra 'hàn' không khớp 'hàng', 'nhật' không khớp 'cập nhật', 'sạc chính hãng' không tính máy chính hãng."""
    origin_country_re = to_python_re(silver_patterns.ORIGIN_COUNTRY_PATTERN)

    # "hàng đẹp" không được trích xuất thành nước Hàn
    assert origin_country_re.search("hàng đẹp keng nguyên bản") is None

    # "cập nhật win 11" không được trích xuất thành nước Nhật
    assert origin_country_re.search("đã cập nhật win 11 mới nhất") is None

    # Hàng xách tay chuẩn có từ dẫn
    m_us = origin_country_re.search("hàng xách tay mỹ xịn")
    assert m_us is not None
    assert "mỹ" in m_us.group(1)

    m_jp = origin_country_re.search("máy nội địa nhật bản")
    assert m_jp is not None
    assert "nhật" in m_jp.group(1)


def test_condition_and_battery_separation():
    """Kiểm tra không bị nhầm pin 95% thành độ mới ngoại hình máy 95%."""
    cond_re = to_python_re(silver_patterns.CONDITION_PCT_PATTERN)
    bat_re = to_python_re(silver_patterns.BATTERY_PCT_PATTERN)

    # "pin 95%" phải thuộc về battery, không phải ngoại hình
    assert cond_re.search("máy trầy nhẹ, pin 95% trâu") is None
    m_bat = bat_re.search("máy trầy nhẹ, pin 95% trâu")
    assert m_bat is not None
    assert m_bat.group(1) == "95"

    # Ngoại hình 95%
    m_cond = cond_re.search("ngoại hình 95% còn đẹp")
    assert m_cond is not None
    assert m_cond.group(1) == "95"


def test_repair_status_and_upgrades():
    """Kiểm tra 'sạc zin' không coi máy zin; 'thay ssd' là nâng cấp chứ không phải sửa chữa hư hỏng."""
    orig_re = to_python_re(silver_patterns.REPAIR_ORIGINAL_PATTERN)
    upgrade_re = to_python_re(silver_patterns.UPGRADE_PATTERN)

    # "kèm sạc zin" không tính là máy nguyên zin
    assert orig_re.search("bán máy kèm sạc zin theo máy") is None

    # "máy zin keng"
    assert orig_re.search("máy nguyên zin chưa qua sửa chữa") is not None

    # "thay ssd 512gb" là nâng cấp linh kiện
    assert upgrade_re.search("đã nâng cấp ram 16gb và thay ssd 512gb") is not None
