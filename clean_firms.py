"""
clean_firms.py
==============
Cleans result_firms.csv:
  - Fixes corrupted latitude/longitude values (dot-as-thousands-separator)
  - Fixes phone numbers stored as floats (restores + prefix, removes .0)
  - Converts year_founded and employee_count to proper integers
  - Formats revenue as readable number (e.g. 1,250,000)
  - Strips leading/trailing whitespace from all string columns
  - Standardises capitalisation on categorical columns (company_type, revenue_type, etc.)
  - Replaces ALL empty / NaN / null-like cells with "-"
  - Saves cleaned file as result_firms_cleaned.csv
"""

import pandas as pd
import numpy as np
import re
import os

INPUT  = "/mnt/user-data/uploads/result_firms.csv"
OUTPUT = "/mnt/user-data/outputs/result_firms_cleaned.csv"

print("Loading data...")
df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)
print(f"  Shape: {df.shape[0]} rows × {df.shape[1]} columns")

print("Normalising blank cells...")
df.replace(r"^\s*$", np.nan, regex=True, inplace=True)
df.replace("nan", np.nan, inplace=True)
df.replace("NaN", np.nan, inplace=True)
df.replace("None", np.nan, inplace=True)
df.replace("NULL", np.nan, inplace=True)

print("Stripping whitespace...")
for col in df.columns:
    df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)


def fix_coord(val):
    if pd.isna(val):
        return np.nan
    val = str(val).strip()
    parts = val.split(".")
    if len(parts) <= 2:
        try:
            return str(round(float(val), 8))
        except ValueError:
            return np.nan
    integer_part  = parts[0]
    decimal_parts = parts[1:]
    decimal_str   = "".join(decimal_parts)
    reconstructed = f"{integer_part}.{decimal_str[:8]}"
    try:
        return str(round(float(reconstructed), 8))
    except ValueError:
        return np.nan

print("Fixing latitude/longitude...")
df["main_latitude"]  = df["main_latitude"].apply(fix_coord)
df["main_longitude"] = df["main_longitude"].apply(fix_coord)


def fix_phone(val):
    if pd.isna(val):
        return np.nan
    val = str(val).strip()
    val = re.sub(r"\.0+$", "", val)
    val = val.lstrip("+")
    val = re.sub(r"[\s\-()]", "", val)
    if not val or not val.lstrip("-").isdigit():
        return np.nan
    return f"+{val}"

print("Fixing phone numbers...")
df["primary_phone"] = df["primary_phone"].apply(fix_phone)

def fix_phone_list(val):
    if pd.isna(val):
        return np.nan
    items = [fix_phone(p) for p in str(val).split("|")]
    items = [i for i in items if i and not pd.isna(i)]
    return " | ".join(items) if items else np.nan

df["phone_numbers"] = df["phone_numbers"].apply(fix_phone_list)

print("Fixing year_founded...")
def fix_year(val):
    if pd.isna(val):
        return np.nan
    try:
        y = int(float(str(val)))
        if 1800 <= y <= 2025:
            return str(y)
        return np.nan
    except (ValueError, OverflowError):
        return np.nan

df["year_founded"] = df["year_founded"].apply(fix_year)

print("Fixing employee_count...")
def fix_int_col(val):
    if pd.isna(val):
        return np.nan
    try:
        return str(int(float(str(val))))
    except (ValueError, OverflowError):
        return np.nan

df["employee_count"] = df["employee_count"].apply(fix_int_col)
df["num_locations"]  = df["num_locations"].apply(fix_int_col)

print("Formatting revenue...")
def fix_revenue(val):
    if pd.isna(val):
        return np.nan
    try:
        r = float(str(val))
        return f"{int(r):,}"
    except (ValueError, OverflowError):
        return np.nan

df["revenue"] = df["revenue"].apply(fix_revenue)

print("Standardising categorical columns...")
categorical_cols = [
    "company_type", "revenue_type", "employee_count_type",
    "main_business_category", "main_industry", "main_sector",
    "website_tld", "website_language_code",
]
for col in categorical_cols:
    if col in df.columns:
        df[col] = df[col].apply(
            lambda x: x.strip().title() if isinstance(x, str) else x
        )

url_cols = [
    "website_url", "website_domain", "facebook_url",
    "twitter_url", "instagram_url", "linkedin_url",
    "ios_app_url", "android_app_url", "youtube_url", "tiktok_url",
]
for col in url_cols:
    if col in df.columns:
        df[col] = df[col].apply(
            lambda x: x.strip().lower() if isinstance(x, str) else x
        )

code_cols = [
    "naics_2022_primary_code", "sic_codes", "isic_v4_codes",
    "nace_rev2_codes", "ibc_insurance_codes", "sics_codified_industry_code",
    "sics_codified_subsector_code", "sics_codified_sector_code",
]
for col in code_cols:
    if col in df.columns:
        df[col] = df[col].apply(
            lambda x: re.sub(r"\.0\b", "", str(x)).strip() if isinstance(x, str) else x
        )

print("Replacing all empty cells with '-'...")
def to_dash(val):
    if val is None:
        return "-"
    s = str(val).strip()
    if s in ("", "nan", "NaN", "None", "NULL", "none", "null", "<NA>"):
        return "-"
    return s

for col in df.columns:
    df[col] = df[col].apply(to_dash)

os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
print(f"\nCleaned file saved → {OUTPUT}")

print("\n── CLEANING SUMMARY ─────────────────────────────────────────────────")
print(f"  Rows           : {df.shape[0]}")
print(f"  Columns        : {df.shape[1]}")

dash_counts = (df == "-").sum()
total_cells = df.shape[0] * df.shape[1]
total_dashes = dash_counts.sum()
print(f"  Total cells    : {total_cells:,}")
print(f"  Cells set to - : {total_dashes:,}  ({100*total_dashes/total_cells:.1f}%)")
print()
print("  Top columns with missing data (shown as '-'):")
top_missing = dash_counts[dash_counts > 0].sort_values(ascending=False).head(15)
for col, cnt in top_missing.items():
    pct = 100 * cnt / df.shape[0]
    print(f"    {col:<40} {cnt:>4} rows  ({pct:.0f}%)")
print()
print("  Sample cleaned rows (key columns):")
sample_cols = [
    "input_company_name", "company_name", "main_country",
    "year_founded", "revenue", "employee_count",
    "primary_phone", "primary_email", "website_url",
    "main_latitude", "main_longitude",
]
print(df[sample_cols].head(5).to_string(index=False))
print("\nDone.")
