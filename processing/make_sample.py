"""Small development sample (brief 6.5): in-scope rows + some out-of-scope rows + known bad records.
Writes tests/data/sample_complaints.csv (small, committed to Git).

docker compose run --rm jobs python -m processing.make_sample --ingest-date YYYY-MM-DD
"""
import argparse
import glob
import shutil

from pyspark.sql import functions as F

from config import settings
from ingestion.storage import get_json
from processing.schema import explicit_string_schema
from processing.spark_session import get_spark

parser = argparse.ArgumentParser()
parser.add_argument("--ingest-date", required=True)
parser.add_argument("--in-scope", type=int, default=1500)
parser.add_argument("--out-of-scope", type=int, default=500)
args = parser.parse_args()

prefix = f"cfpb/full_load/ingest_date={args.ingest_date}"
manifest = get_json(settings.BUCKET_RAW, f"{prefix}/manifest.json")
spark = get_spark("cfpb-sample")
df = (spark.read.schema(explicit_string_schema(manifest["header"])).option("header", True)
      .option("multiLine", True).option("escape", '"').csv(f"s3a://raw/{prefix}/complaints.csv"))

cond = None
for f in settings.SCOPE_FILTERS:
    c = F.col("Product") == f["product"]
    if f["sub_product"]:
        c = c & (F.col("Sub-product") == f["sub_product"])
    cond = c if cond is None else cond | c

inside = df.filter(cond).orderBy(F.rand(42)).limit(args.in_scope)
# out-of-scope rows from the same parent products (title loans etc.) prove the filter works
outside = df.filter(~cond & F.col("Product").startswith("Payday")).orderBy(F.rand(42)).limit(args.out_of_scope)
sample = inside.unionByName(outside)

# known bad test records (brief section 10): invalid date, missing ID, duplicate ID
first = inside.limit(3).collect()
bad = [first[0].asDict(), first[1].asDict(), first[2].asDict()]
bad[0]["Date received"] = "2024-13-45"; bad[0]["Complaint ID"] = "999999901"
bad[1]["Complaint ID"] = None
bad[2]["Company response to consumer"] = "Closed with monetary relief"     # same ID, conflicting value
sample = sample.unionByName(spark.createDataFrame(bad, schema=sample.schema))

sample.coalesce(1).write.mode("overwrite").option("header", True).csv("/tmp/sample_out")
shutil.copy(glob.glob("/tmp/sample_out/part-*.csv")[0], "tests/data/sample_complaints.csv")
print(f"written tests/data/sample_complaints.csv ({sample.count()} rows incl. 3 known bad records)")
