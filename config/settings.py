"""Central configuration. Everything comes from environment variables (.env) and config/*.json."""
import json
import os

_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "cfpb")

# --- Project option (brief section 4) ---
OPTION_NUMBER = int(os.getenv("OPTION_NUMBER", "11"))
SCOPE_FILE = os.path.join(_CONFIG_DIR, f"scope_option{OPTION_NUMBER}.json")
with open(SCOPE_FILE, encoding="utf-8") as _fh:
    SCOPE = json.load(_fh)
OPTION_NAME = SCOPE["option_name"]
SCOPE_FILTERS = SCOPE["filters"]              # used by Spark (product + optional sub-product)
API_PRODUCT_PARAMS = SCOPE["api_product_params"]  # used by the API collector

CFPB_CSV_URL = os.getenv("CFPB_CSV_URL", "https://files.consumerfinance.gov/ccdb/complaints.csv.zip")
CFPB_API_URL = os.getenv(
    "CFPB_API_URL",
    "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/",
)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "21"))
API_PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "1000"))
API_PAUSE_SECONDS = float(os.getenv("API_PAUSE_SECONDS", "0.5"))

BUCKET_RAW, BUCKET_CURATED, BUCKET_QUALITY, BUCKET_OPS = "raw", "curated", "quality", "ops"
# Brief 6.1: curated/complaints/option=<number>/year=YYYY/month=MM/
CURATED_PATH = f"s3a://{BUCKET_CURATED}/complaints/option={OPTION_NUMBER}"
WATERMARK_KEY = f"state/option={OPTION_NUMBER}/watermark.json"
