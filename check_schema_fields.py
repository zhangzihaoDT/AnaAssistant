import pandas as pd
from pathlib import Path

# Load actual columns from datasets
parquet_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
csv_path = "/Users/zihao_/Documents/coding/dataset/original/assign_data.csv"

print(f"Loading columns from: {parquet_path}")
try:
    df_parquet = pd.read_parquet(parquet_path)
    parquet_cols = set(df_parquet.columns)
except Exception as e:
    print(f"Error loading parquet: {e}")
    parquet_cols = set()

print(f"Loading columns from: {csv_path}")
try:
    # Use safe read logic similar to tool
    candidates = [
        {"encoding": "utf-8", "sep": ","},
        {"encoding": "utf-16", "sep": "\t"},
        {"encoding": "utf-16le", "sep": "\t"},
        {"encoding": "gbk", "sep": ","},
    ]
    df_csv = None
    for opt in candidates:
        try:
            df_csv = pd.read_csv(csv_path, **opt)
            break
        except:
            continue
    
    if df_csv is not None:
        csv_cols = set(df_csv.columns)
    else:
        print("Failed to load CSV with standard options")
        csv_cols = set()
except Exception as e:
    print(f"Error loading CSV: {e}")
    csv_cols = set()

all_cols = parquet_cols.union(csv_cols)

# List of fields defined in schema.md (manually extracted from the file content provided in context)
# Sections: Time Dimensions, Metrics (fields used in conditions), Dimensions
defined_fields = [
    # Time Dimensions
    "order_create_time", "order_create_date", "store_create_date", "lock_time",
    "invoice_upload_time", "delivery_date", "intention_payment_time", "intention_refund_time",
    "deposit_payment_time", "deposit_refund_time", "apply_refund_time", "approve_refund_time",
    "first_touch_time", "first_test_drive_time", "lead_assign_time_max", "first_assign_time",
    "Assign Time 年/月/日",
    
    # Metrics (fields used)
    "order_number", "invoice_amount", "age", "td_countd",
    "下发线索数", "下发线索当日试驾数", "下发线索 7 日试驾数", "下发线索 7 日锁单数",
    "下发线索 30日试驾数", "下发线索 30 日锁单数", "下发门店数", "下发线索数 (门店)",
    "下发线索当日锁单数 (门店)",
    
    # Dimensions
    "product_name", "series", "belong_intent_series", "drive_series_cn", "product_type",
    "store_city", "store_name", "parent_region_name", "license_province", "license_city",
    "license_city_level", "first_middle_channel_name", "gender", "is_staff", "is_hold",
    "order_type", "finance_product", "final_payment_way", "main_lead_id"
]

print(f"\nChecking {len(defined_fields)} defined fields against actual dataset columns...")
print("-" * 60)

missing_fields = []
for field in defined_fields:
    # product_type is known to be missing/derived, skip check if we want, or just let it show up
    if field not in all_cols:
        print(f"❌ Missing in datasets: {field}")
        missing_fields.append(field)
    else:
        # print(f"✅ Found: {field}")
        pass

print("-" * 60)
print(f"Total missing fields: {len(missing_fields)}")
