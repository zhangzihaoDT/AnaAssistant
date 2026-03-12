import pandas as pd
import datetime
import sys

file_path = "/Users/zihao_/Documents/coding/dataset/formatted/order_full_data.parquet"
print(f"Loading data from: {file_path}")

df = pd.read_parquet(file_path)

year_arg = None
if len(sys.argv) >= 2 and str(sys.argv[1]).strip().isdigit():
    year_arg = int(str(sys.argv[1]).strip())
target_year = year_arg or (datetime.date.today().year - 1)

df["lock_time"] = pd.to_datetime(df["lock_time"], errors="coerce")

year_start = pd.Timestamp(f"{target_year}-01-01")
year_end = pd.Timestamp(f"{target_year + 1}-01-01")
df_year = df[(df["lock_time"] >= year_start) & (df["lock_time"] < year_end)]

city_values = ["南京", "南京市"]
store_city_in = df_year["store_city"].isin(city_values)
license_city_in = df_year["license_city"].isin(city_values)

store_city_df = df_year[store_city_in]
license_city_df = df_year[license_city_in]
either_city_df = df_year[store_city_in | license_city_in]
both_city_df = df_year[store_city_in & license_city_in]

print(f"\nLock count in year {target_year} (lock_time in [{year_start.date()}, {year_end.date()}))")
print(f"- Total locked orders (all): {len(df_year)}")
print(f"- store_city in {city_values}: {len(store_city_df)}")
print(f"- license_city in {city_values}: {len(license_city_df)}")
print(f"- either store_city or license_city in {city_values}: {len(either_city_df)}")
print(f"- both store_city and license_city in {city_values}: {len(both_city_df)}")

print("\nTop store_city values within year window:")
print(df_year["store_city"].value_counts().head(20).to_string())

print("\nTop license_city values within year window:")
print(df_year["license_city"].value_counts().head(20).to_string())

print("\nMatched store_city value counts:")
print(store_city_df["store_city"].value_counts(dropna=False).to_string())

print("\nMatched license_city value counts:")
print(license_city_df["license_city"].value_counts(dropna=False).to_string())
