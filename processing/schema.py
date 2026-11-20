"""Source schemas (CSV and API) and the canonical curated schema."""
import re

from pyspark.sql.types import StringType, StructField, StructType

# Headers the official CSV must contain (field reference, checked 17-20 Nov 2026).
# All are read as strings first; types are parsed and validated afterwards.
REQUIRED_CSV_HEADERS = [
    "Date received", "Product", "Sub-product", "Issue", "Sub-issue", "Company public response",
    "Company", "State", "ZIP code", "Tags", "Submitted via", "Date sent to company",
    "Company response to consumer", "Timely response?", "Complaint ID",
]
# Columns that may still appear in older exports but are removed from the source and not used.
KNOWN_RETIRED_HEADERS = ["Consumer complaint narrative", "Consumer consent provided?", "Consumer disputed?"]


def explicit_string_schema(headers):
    """Explicit schema: every column as nullable string, in the order of the file header."""
    return StructType([StructField(h, StringType(), True) for h in headers])


def validate_header(headers):
    """Returns (missing_required, unexpected). Missing required columns must stop the run."""
    missing = [h for h in REQUIRED_CSV_HEADERS if h not in headers]
    unexpected = [h for h in headers if h not in REQUIRED_CSV_HEADERS and h not in KNOWN_RETIRED_HEADERS]
    return missing, unexpected


# Canonical snake_case names shared by the CSV path and the API path
CANONICAL_COLUMNS = [
    "complaint_id", "date_received", "date_sent_to_company",
    "product", "sub_product", "issue", "sub_issue",
    "company", "company_public_response", "company_response_to_consumer", "timely_response",
    "state", "zip_code", "submitted_via", "tags",
]
REQUIRED_SOURCE_COLUMNS = ["complaint_id", "date_received", "product"]

# API field names that differ from the normalised CSV headers
ALIASES = {"company_response": "company_response_to_consumer", "timely": "timely_response"}


def canonical_name(raw: str) -> str:
    """'Timely response?' -> 'timely_response', 'Sub-product' -> 'sub_product'."""
    name = re.sub(r"[^0-9a-z]+", "_", raw.strip().lower()).strip("_")
    return ALIASES.get(name, name)


VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
    "PR", "GU", "VI", "AS", "MP", "UM", "FM", "MH", "PW",   # territories / freely associated states
    "AA", "AE", "AP",                                         # military mail codes
}
