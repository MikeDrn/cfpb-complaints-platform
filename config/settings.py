"""Central configuration. Everything comes from environment variables (.env)."""
import os


def _list(name: str) -> list[str]:
    return [p.strip() for p in os.getenv(name, "").split("|") if p.strip()]


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "cfpb")

DOMAIN_NAME = os.getenv("DOMAIN_NAME", "all")
DOMAIN_PRODUCTS = _list("DOMAIN_PRODUCTS")

CFPB_CSV_URL = os.getenv("CFPB_CSV_URL", "https://files.consumerfinance.gov/ccdb/complaints.csv.zip")
CFPB_API_URL = os.getenv(
    "CFPB_API_URL",
    "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/",
)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "21"))
API_PAGE_SIZE = int(os.getenv("API_PAGE_SIZE", "100"))
API_PAUSE_SECONDS = float(os.getenv("API_PAUSE_SECONDS", "0.5"))

BUCKET_RAW, BUCKET_CURATED, BUCKET_QUALITY, BUCKET_OPS = "raw", "curated", "quality", "ops"
CURATED_PATH = f"s3a://{BUCKET_CURATED}/complaints/domain={DOMAIN_NAME}"
WATERMARK_KEY = "state/watermark.json"