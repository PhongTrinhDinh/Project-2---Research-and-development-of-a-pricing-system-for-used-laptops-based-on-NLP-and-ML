import unicodedata
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, lower, trim, regexp_replace, regexp_extract, when, coalesce, lit, udf, year, months_between, to_date
from pyspark.sql.types import StringType

# ==============================================================================
# 1. Unicode Word Boundaries & Regex Helpers
# ==============================================================================
# Java regex engine in Spark supports \p{L} (Unicode Letter) and \p{N} (Unicode Number).
# Using (?<![\p{L}\p{N}]) and (?![\p{L}\p{N}]) prevents false positives like:
# 'hàn' in 'hàng', 'nhật' in 'cập nhật', 'touch' in 'touchpad'.
L = r"(?<![\p{L}\p{N}])"
R = r"(?![\p{L}\p{N}])"

def w(pattern: str) -> str:
    """Bọc mẫu regex bằng ranh giới từ Unicode an toàn cho tiếng Việt."""
    return f"{L}(?:{pattern}){R}"

def null_if_empty(c: Column) -> Column:
    """Trả về NULL nếu kết quả là chuỗi rỗng ''."""
    return when(c != lit(""), c).otherwise(lit(None))

def extract_safe(col_ref: Column, pattern: str, group: int = 1) -> Column:
    """Trích xuất regexp_extract và chuyển chuỗi rỗng thành NULL."""
    return null_if_empty(regexp_extract(col_ref, pattern, group))

def extract_pref(title_col: Column, desc_col: Column, pattern: str, group: int = 1, return_source: bool = False):
    """
    Ưu tiên trích xuất từ Title trước, nếu không có mới lấy từ Description.
    Nếu return_source=True, trả về tuple (value_col, source_col).
    """
    val_title = extract_safe(title_col, pattern, group)
    val_desc = extract_safe(desc_col, pattern, group)
    val_combined = coalesce(val_title, val_desc)

    if not return_source:
        return val_combined

    source_col = (
        when(val_title.isNotNull(), lit("title"))
        .when(val_desc.isNotNull(), lit("description"))
        .otherwise(lit(None))
    )
    return val_combined, source_col


# ==============================================================================
# 2. Text Normalization (NFC, NBSP, Whitespace)
# ==============================================================================
@udf(returnType=StringType())
def normalize_nfc_udf(text):
    if text is None:
        return None
    return unicodedata.normalize("NFC", text)

def normalize_text_column(col_ref: Column) -> Column:
    """
    Chuẩn hóa văn bản:
    1. Chuyển NFC (gộp dấu tiếng Việt tổ hợp về dạng dựng sẵn).
    2. Thay thế non-breaking spaces và khoảng trắng đặc biệt.
    3. Đưa về chữ thường.
    4. Gộp nhiều khoảng trắng liên tiếp và trim.
    """
    # Thay thế NBSP và khoảng trắng Unicode
    c = regexp_replace(col_ref, r"[\u00A0\u2000-\u200B\u202F\u205F\u3000\uFEFF]", " ")
    c = lower(c)
    c = regexp_replace(c, r"\s+", " ")
    c = trim(c)
    return normalize_nfc_udf(c)


# ==============================================================================
# 3. Mappings & Regex Patterns
# ==============================================================================

# ----------------- Brand & Product Line -----------------
BRAND_DIRECT_PATTERN = w(r"hp|dell|asus|acer|lenovo|apple|macbook|msi|microsoft|samsung|lg|toshiba|fujitsu|huawei|xiaomi")

PRODUCT_LINE_PATTERN = w(
    r"elitebook(?:\s*\d{3,4})?(?:\s*g\d{1,2})?|"
    r"probook(?:\s*\d{3,4})?(?:\s*g\d{1,2})?|"
    r"pavilion(?:\s*(?:14|15|16|gaming|aero))?|"
    r"envy(?:\s*(?:13|14|15|16|x360))?|"
    r"victus(?:\s*(?:15|16))?|"
    r"omen(?:\s*(?:15|16|17))?|"
    r"zbook(?:\s*(?:firefly|power|fury|studio))?|"
    r"latitude(?:\s*[a-z]?\d{3,4})?|"
    r"inspiron(?:\s*\d{3,4})?|"
    r"vostro(?:\s*\d{3,4})?|"
    r"xps(?:\s*(?:13|14|15|16|17))?(?:\s*(?:plus|2in1))?|"
    r"precision(?:\s*\d{4})?|"
    r"alienware(?:\s*(?:m15|m16|m17|m18|x14|x15|x16|x17))?|"
    r"thinkpad(?:\s*(?:x1\s*(?:carbon|yoga|nano|extreme)|[a-z]\d{2,3}[a-z]?|t\d{3}[a-z]?|p\d{2,3}[a-z]?|e\d{3}[a-z]?|l\d{3}[a-z]?))?|"
    r"ideapad(?:\s*(?:gaming\s*3|slim\s*\d|flex\s*\d|\d{3,4}))?|"
    r"legion(?:\s*(?:5\s*pro|5|7|slim|pro\s*5|pro\s*7))?|"
    r"loq(?:\s*\d{2})?|"
    r"thinkbook(?:\s*(?:13s|14s|14|15|16))?|"
    r"yoga(?:\s*(?:slim\s*\d|\d{3,4}))?|"
    r"vivobook(?:\s*(?:s14|s15|pro|flip|14|15|16))?|"
    r"zenbook(?:\s*(?:pro|flip|duo|13|14|15))?|"
    r"tuf(?:\s*(?:gaming)?(?:\s*(?:f15|f17|a15|a16|dash))?)?|"
    r"rog(?:\s*(?:strix|zephyrus|flow))?|"
    r"expertbook(?:\s*[a-z]\d{3,4})?|"
    r"nitro(?:\s*(?:5|16|17))?|"
    r"aspire(?:\s*(?:3|5|7))?|"
    r"swift(?:\s*(?:3|5|go|x))?|"
    r"predator(?:\s*(?:helios|triton))?|"
    r"surface(?:\s*(?:pro|laptop|book|go))?(?:\s*\d{1,2})?|"
    r"macbook\s*(?:pro|air)"
)

# Ánh xạ từ dòng máy sang thương hiệu
BRAND_FROM_LINE_CONDITIONS = [
    (r"vivobook|zenbook|rog|tuf|expertbook", "asus"),
    (r"inspiron|latitude|xps|vostro|precision|alienware", "dell"),
    (r"pavilion|elitebook|probook|envy|victus|omen|zbook", "hp"),
    (r"thinkpad|ideapad|legion|loq|thinkbook|yoga", "lenovo"),
    (r"nitro|aspire|swift|predator", "acer"),
    (r"macbook", "apple"),
    (r"surface", "microsoft"),
]

# ----------------- CPU Patterns -----------------
# Intel Core i: i5 1135g7, i7-12700h, i5 8250u...
# Ngăn chặn khớp nhầm storage như 'i5 512gb' bằng cách ràng buộc hậu tố CPU hoặc \b không chứa gb/tb
INTEL_MODEL_PATTERN = w(
    r"(?:core\s*)?(i[3579][-\s]?(?:(?:1[0-5]\d{3}|\d{3,4})(?:g[1-7]|hx|hk|hq|hs|h|u|y|p|k|f|t)?))(?![a-zA-Z0-9])"
)
INTEL_GEN_PHRASE_PATTERN = w(
    r"(?:core\s*)?i[3579][-\s]?(?:gen\s*(\d{1,2})|thế\s*hệ\s*(\d{1,2})|th\s*(\d{1,2}))"
)
INTEL_ULTRA_PATTERN = w(r"(?:core\s*)?ultra\s*([579][-\s]?\d{3}[hu]?)")

# AMD Ryzen: ryzen 7 pro 4750u, ryzen 5 5600hs, r5 5600h
AMD_MODEL_PATTERN = w(
    r"(?:amd\s*)?(ryzen\s*[3579](?:\s*pro)?(?:\s*\d{4}(?:hx|hs|h|u|ge|g|x3d|x)?)?|r[3579][-\s]?\d{4}(?:hx|hs|h|u|ge|g|x3d|x)?)"
)

# Apple Silicon: M1, M2 Pro, M3 Max, M4
# Loại trừ M.2 NVMe: (?![\s-]*(?:nvme|sata|ssd|pcie|2280|2242|2230|\.2))
APPLE_M_PATTERN = w(
    r"(m[1-4](?:\s*(?:pro|max|ultra))?)(?![\s-]*(?:nvme|sata|ssd|pcie|2280|2242|2230|\.2))"
)

# ----------------- GPU Patterns -----------------
GPU_DISCRETE_PATTERN = w(
    r"(?:nvidia\s*)?((?:rtx|gtx)\s*\d{3,4}\s*(?:ti|super)?\s*m?|"
    r"mx\s*\d{3}|"
    r"rtx\s*a\d{3,4}|"
    r"quadro\s*[a-z]?\d{3,4}|"
    r"radeon\s*(?:rx\s*)?\d{3,4}[a-z]*|"
    r"intel\s*arc\s*[a-z]?\d{3})"
)

GPU_INTEGRATED_PATTERN = w(
    r"(iris\s*xe|"
    r"uhd(?:\s*graphics)?(?:\s*\d{3})?|"
    r"intel\s*hd(?:\s*graphics)?(?:\s*\d{3,4})?|"
    r"radeon\s*(?:graphics|vega\s*\d*)|"
    r"apple\s*gpu|m[1-4]\s*(?:gpu|\d+\s*core))"
)

# ----------------- Screen Size & Resolution -----------------
# Hỗ trợ cả 15,6 inch lẫn 15.6 và tin không ghi đơn vị như 'laptop 15.6 fhd'
SCREEN_SIZE_PATTERN = w(
    r"(\d{2}(?:[.,]\d)?)\s*(?:inch|in|\"|”|''|(?=\s*(?:fhd|hd|qhd|2k|3k|4k|oled|ips|retina|màn)))"
)

# Độ phân giải dạng số: 1920x1080, 1920x1200, 2560x1440, 2880x1800, 3840x2160
RESOLUTION_NUM_PATTERN = w(r"(\d{3,4})\s*[x×*]\s*(\d{3,4})")

# Độ phân giải dạng chữ
RESOLUTION_TEXT_PATTERN = w(
    r"(fhd\+|wuxga|full\s*hd\+|fhd|full\s*hd|qhd\+|wqhd|qhd|retina|uhd|"
    r"(?:màn|màn\s*hình|độ\s*phân\s*giải|oled|ips|retina)[\s:-]{1,5}(?:2k|2\.5k|3k|4k)|"
    r"(?:2k|2\.5k|3k|4k)[\s-]*(?:oled|ips|retina|120hz|144hz|165hz|60hz|màn|hdr))"
)

# ----------------- Touch Sensitive -----------------
# 1 nếu có cảm ứng, 0 nếu nói rõ không có cảm ứng, NULL nếu không nhắc tới
TOUCH_POS_PATTERN = w(r"cảm\s*ứng|touchscreen|màn\s*(?:hình\s*)?touch|touch(?![\s-]*(?:pad|bar|id))")
TOUCH_NEG_PATTERN = w(r"(?:không|ko|k)\s*cảm\s*ứng|non[-\s]?touch")

# ----------------- Condition & Likely New -----------------
NEW_SEAL_PATTERN = w(r"nguyên\s*seal|new\s*seal|chưa\s*active|chưa\s*kích\s*hoạt|mới\s*100%|full\s*box\s*mới")
LIKE_NEW_PATTERN = w(r"like\s*new|như\s*mới|đẹp\s*keng|keng\s*xà\s*bà|máy\s*keng")
# Bắt tỷ lệ % độ mới của máy (80-99%), loại trừ pin và gam màu
CONDITION_PCT_PATTERN = w(
    r"(?:máy|ngoại\s*hình|vỏ|độ\s*mới|hình\s*thức)\s*[:\-]?\s*([89]\d|100)\s*%"
)

# ----------------- Origin -----------------
# Nước xuất xứ: mỹ, nhật, hàn, úc, đức, vn, sing
# Phải có từ dẫn để không khớp 'hàng' trong 'hàng đẹp' hay 'nhật' trong 'cập nhật'
ORIGIN_COUNTRY_PATTERN = w(
    r"(?:hàng\s+xách\s+tay|hàng|xách\s*tay|nhập|nội\s*địa|bãi|từ|bản|máy)\s*(mỹ|nhật(?:\s*bản)?|hàn(?:\s*quốc)?|úc|đức|singapore|sing|châu\s*âu)|"
    r"(ll/a|j/a|kh/a|vn/a|za/a)"
)

ORIGIN_TYPE_PATTERN = w(
    r"(chính\s*hãng|fpt|tgdd|thế\s*giới\s*di\s*động|viettel|vn/a|"
    r"xách\s*tay|hàng\s*bãi|nhập\s*khẩu|ll/a|j/a|kh/a)"
)

# ----------------- Battery -----------------
BATTERY_PCT_PATTERN = w(
    r"(?:pin\s*(?:còn|dung\s*lượng|khoảng|sức\s*khỏe)?\s*[:\-]?\s*)(\d{2,3})\s*%|"
    r"(\d{2,3})\s*%\s*pin"
)
BATTERY_CYCLES_PATTERN = w(
    r"(?:cycle\s*count|số\s*lần\s*sạc|chu\s*k[ỳy]\s*sạc|lần\s*sạc|cycle)\s*[:\-]?\s*(\d{1,4})"
)
BATTERY_HOURS_PATTERN = w(
    r"pin\s*(?:dùng|trâu|tầm|được|liên\s*tục)?\s*[:\-]?\s*(\d{1,2}(?:[.,]\d)?)\s*(?:tiếng|h|giờ)"
)
BATTERY_WORN_PATTERN = w(r"chai\s*pin|pin\s*chai|pin\s*yếu|phồng\s*pin|báo\s*bảo\s*trì")
BATTERY_REPLACED_PATTERN = w(r"đã\s*thay\s*pin|pin\s*mới\s*thay|thay\s*pin\s*mới")
BATTERY_GOOD_PATTERN = w(r"pin\s*(?:trâu|tốt|ngon|hoàn\s*hảo|good)")

# ----------------- Warranty -----------------
WARRANTY_MONTHS_PATTERN = w(r"(?:bảo\s*hành|bh)\s*[:\-]?\s*(?:còn\s*)?(\d{1,2})\s*(?:tháng|th)(?![\p{L}])")
WARRANTY_YEARS_PATTERN = w(r"(?:bảo\s*hành|bh)\s*[:\-]?\s*(?:còn\s*)?(\d{1,2})\s*năm")
WARRANTY_EXPIRED_PATTERN = w(r"(?:hết|không|ko|k)\s*(?:bảo\s*hành|bh)|bh\s*(?:0\s*tháng|hết)")
WARRANTY_TYPE_PATTERN = w(r"(chính\s*hãng|applecare|hãng|care\+|cửa\s*hàng|shop|bao\s*test)")

# ----------------- Repair & Upgrade -----------------
UPGRADE_PATTERN = w(r"nâng\s*cấp\s*(?:ram|ssd|ổ)|lên\s*(?:ram|ssd|ổ)|(?:thay|gắn\s*thêm|lắp\s*thêm|thêm)\s*(?:ram|ssd|ổ\s*cứng)")
# Máy zin / nguyên bản (không tính sạc zin, tai nghe zin)
REPAIR_ORIGINAL_PATTERN = w(
    r"(?:máy\s*zin|nguyên\s*zin|zin\s*all|zin\s*keng|nguyên\s*bản|chưa\s*(?:qua\s*)?sửa\s*(?:chữa)?|chưa\s*bung|zin\s*100%)"
)
REPAIR_FIXED_PATTERN = w(
    r"đã\s*(?:qua\s*)?sửa\s*(?:chữa)?|đã\s*bung|thay\s*(?:main|màn|màn\s*hình|bo|chip|vỏ|bản\s*lề|bàn\s*phím|loa)"
)

# ----------------- Year -----------------
YEAR_PRODUCTION_PATTERN = w(
    r"(?:đời|năm\s*sản\s*xuất|sản\s*xuất|sx|model|ra\s*mắt)\s*[:\-]?\s*(20[12]\d)|"
    r"macbook\s*(?:pro|air)[^.\n]{0,15}?(20[12]\d)"
)
YEAR_PURCHASE_PATTERN = w(r"(?:mua|mua\s*mới|sắm)\s*(?:từ|vào|năm)?\s*(?:tháng\s*\d{1,2}/)?(20[12]\d)")

# ----------------- Bundles -----------------
BUNDLES_PATTERN = r"(?:tặng\s*kèm|tặng|quà\s*tặng)[\s:]+([^\n.,;]+)"
