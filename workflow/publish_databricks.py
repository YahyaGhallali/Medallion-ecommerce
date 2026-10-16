import os
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

GOLD_DIR = PROJECT_ROOT / "data" / "gold"
VOLUME = "/Volumes/olist/gold/landing"

raw_host = os.environ.get("DATABRICKS_HOST", "").strip()
token = os.environ.get("DATABRICKS_TOKEN", "").strip()

if not raw_host or not token:
    raise ValueError(
        "Missing DATABRICKS_HOST or DATABRICKS_TOKEN.\n"
        f"Please check that '{PROJECT_ROOT / '.env'}' contains both variables."
    )

parsed = urlparse(raw_host)
host = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else raw_host.rstrip("/")

print(f"Connecting to Databricks workspace: {host}")
w = WorkspaceClient(host=host, token=token)
user = w.current_user.me()
print(f"Authenticated as: {user.user_name}")

if not GOLD_DIR.exists():
    raise FileNotFoundError(f"Gold directory not found at {GOLD_DIR}")

for table_dir in sorted(GOLD_DIR.iterdir()):
    if not table_dir.is_dir():
        continue
    table_name = table_dir.name.removesuffix(".parquet")
    parquet_files = list(table_dir.glob("*.parquet"))
    if not parquet_files:
        continue
    print(f"\nPublishing {table_name} ({len(parquet_files)} file(s))...")
    for f in parquet_files:
        target = f"{VOLUME}/{table_name}/{f.name}"
        with open(f, "rb") as fh:
            w.files.upload(target, fh, overwrite=True)
        print(f"  [OK] Uploaded -> {target}")

print("\nAll Gold datasets published successfully to Databricks Volume.")