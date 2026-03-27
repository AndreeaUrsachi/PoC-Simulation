"""
Company Match & Enrich Pipeline
================================
For each input company (identified by input_row_key), this pipeline:
1. Scores all candidate Veridion rows as potential matches
2. Selects the best match
3. Computes a confidence score across key fields
4. Outputs a single enriched row per company to a new CSV

Confidence scoring dimensions:
  - Name match        (30 pts)  fuzzy string similarity between input and veridion name
  - Country match     (20 pts)  exact ISO code match
  - City match        (15 pts)  fuzzy city name match
  - Has website       (10 pts)  veridion row has a website_url
  - Has phone         (10 pts)  veridion row has a primary_phone
  - Has email         ( 5 pts)  veridion row has a primary_email
  - Has description   ( 5 pts)  veridion row has a short_description
  - Data completeness ( 5 pts)  proportion of non-null enrichment fields
  ─────────────────────────────────
  Total possible      100 pts

Confidence bands:
  HIGH    ≥ 70
  MEDIUM  40–69
  LOW     < 40
"""

import pandas as pd
import numpy as np
import re
import sys
from difflib import SequenceMatcher

# ── helpers ──────────────────────────────────────────────────────────────────

def normalise(text: str) -> str:
    """Lowercase, strip punctuation/common legal suffixes for fuzzy compare."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    # remove common legal suffixes
    suffixes = [
        r"\(private\) limited", r"\(pvt\) ltd", r"private limited",
        r"pvt\.?\s*ltd\.?", r"ltd\.?", r"llc\.?", r"inc\.?",
        r"corp\.?", r"gmbh", r"s\.a\.", r"b\.v\.", r"n\.v\.", r"plc\.?"
    ]
    for s in suffixes:
        text = re.sub(s, "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return " ".join(text.split())


def fuzzy_score(a: str, b: str) -> float:
    """Return 0–1 similarity between two strings."""
    a, b = normalise(a), normalise(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def country_match(a: str, b: str) -> bool:
    """Case-insensitive ISO country code comparison."""
    if not isinstance(a, str) or not isinstance(b, str):
        return False
    return a.strip().upper() == b.strip().upper()


def has_value(val) -> bool:
    return val is not None and isinstance(val, str) and val.strip() != "" or \
           (isinstance(val, float) and not np.isnan(val))


# ── scoring ──────────────────────────────────────────────────────────────────

# Enrichment fields we check for completeness bonus
ENRICH_FIELDS = [
    "company_name", "main_country", "main_city", "main_street",
    "year_founded", "revenue", "employee_count",
    "short_description", "main_industry", "main_sector",
    "website_url", "primary_phone", "primary_email",
    "facebook_url", "linkedin_url", "naics_2022_primary_label",
]


def score_candidate(input_row: pd.Series, candidate: pd.Series) -> dict:
    """
    Score a single Veridion candidate against the input row.
    Returns a dict with individual dimension scores and a total.
    """
    scores = {}

    # 1. Name match (30 pts)
    input_name  = input_row.get("input_company_name", "")
    cand_names  = [candidate.get("company_name", "")]
    # also check legal / commercial names if present
    for field in ("company_legal_names", "company_commercial_names"):
        raw = candidate.get(field, "")
        if isinstance(raw, str):
            cand_names += [n.strip() for n in raw.split("|")]

    best_name_sim = max((fuzzy_score(input_name, n) for n in cand_names), default=0.0)
    scores["name_score"] = round(best_name_sim * 30, 2)

    # 2. Country match (20 pts)
    input_cc  = input_row.get("input_main_country_code", "")
    cand_cc   = candidate.get("main_country_code", "")
    scores["country_score"] = 20.0 if country_match(input_cc, cand_cc) else 0.0

    # 3. City match (15 pts)
    input_city = input_row.get("input_main_city", "")
    cand_city  = candidate.get("main_city", "")
    city_sim   = fuzzy_score(input_city, cand_city)
    scores["city_score"] = round(city_sim * 15, 2)

    # 4. Has website (10 pts)
    scores["website_score"] = 10.0 if has_value(candidate.get("website_url")) else 0.0

    # 5. Has phone (10 pts)
    scores["phone_score"] = 10.0 if has_value(candidate.get("primary_phone")) else 0.0

    # 6. Has email (5 pts)
    scores["email_score"] = 5.0 if has_value(candidate.get("primary_email")) else 0.0

    # 7. Has description (5 pts)
    scores["description_score"] = 5.0 if has_value(candidate.get("short_description")) else 0.0

    # 8. Data completeness (5 pts)
    filled = sum(1 for f in ENRICH_FIELDS if has_value(candidate.get(f)))
    scores["completeness_score"] = round((filled / len(ENRICH_FIELDS)) * 5, 2)

    scores["total_score"] = round(sum(scores.values()), 2)
    return scores


def confidence_band(score: float) -> str:
    if score >= 70:
        return "HIGH"
    elif score >= 40:
        return "MEDIUM"
    return "LOW"


# ── match logic ───────────────────────────────────────────────────────────────

def pick_best_match(input_row: pd.Series, candidates: pd.DataFrame) -> pd.Series:
    """
    Score all candidates and return the best one augmented with scoring columns.
    """
    best_score = -1
    best_idx   = None
    all_scores = []

    for idx, cand in candidates.iterrows():
        sc = score_candidate(input_row, cand)
        all_scores.append((idx, sc))
        if sc["total_score"] > best_score:
            best_score = sc["total_score"]
            best_idx   = idx

    best_cand   = candidates.loc[best_idx].copy()
    best_sc     = dict(next(sc for i, sc in all_scores if i == best_idx))
    num_cands   = len(candidates)

    # Annotate
    best_cand["match_confidence_score"]   = best_sc["total_score"]
    best_cand["match_confidence_band"]    = confidence_band(best_sc["total_score"])
    best_cand["match_name_score"]         = best_sc["name_score"]
    best_cand["match_country_score"]      = best_sc["country_score"]
    best_cand["match_city_score"]         = best_sc["city_score"]
    best_cand["match_website_score"]      = best_sc["website_score"]
    best_cand["match_phone_score"]        = best_sc["phone_score"]
    best_cand["match_email_score"]        = best_sc["email_score"]
    best_cand["match_description_score"]  = best_sc["description_score"]
    best_cand["match_completeness_score"] = best_sc["completeness_score"]
    best_cand["num_candidates_evaluated"] = num_cands

    # Flag obvious mismatches
    flags = []
    if best_sc["name_score"] < 10:
        flags.append("NAME_MISMATCH")
    if best_sc["country_score"] == 0:
        flags.append("COUNTRY_MISMATCH")
    if best_sc["city_score"] < 5:
        flags.append("CITY_MISMATCH")
    best_cand["match_flags"] = "|".join(flags) if flags else "OK"

    return best_cand


# ── main pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(input_path: str, output_path: str) -> None:
    print(f"Loading {input_path} ...")
    df = pd.read_csv(input_path, low_memory=False)
    print(f"  {len(df)} rows, {df['input_row_key'].nunique()} unique companies")

    results = []
    groups  = list(df.groupby("input_row_key"))
    total   = len(groups)

    for i, (row_key, group) in enumerate(groups, 1):
        if i % 50 == 0 or i == total:
            pct = i / total * 100
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            print(f"  [{bar}] {i}/{total}  ({pct:.0f}%)", end="\r", flush=True)

        # All rows in this group share the same input_* columns
        input_row  = group.iloc[0]
        candidates = group.reset_index(drop=True)

        enriched = pick_best_match(input_row, candidates)
        results.append(enriched)

    print()  # newline after progress bar

    out_df = pd.DataFrame(results).reset_index(drop=True)

    # Re-order: input cols → scoring cols → veridion enrichment cols
    input_cols   = [c for c in df.columns if c.startswith("input_")]
    scoring_cols = [
        "match_confidence_score", "match_confidence_band", "match_flags",
        "num_candidates_evaluated",
        "match_name_score", "match_country_score", "match_city_score",
        "match_website_score", "match_phone_score", "match_email_score",
        "match_description_score", "match_completeness_score",
    ]
    enrich_cols  = [c for c in df.columns if c not in input_cols]
    final_cols   = input_cols + scoring_cols + enrich_cols
    # Keep only cols that actually exist in out_df
    final_cols   = [c for c in final_cols if c in out_df.columns]

    out_df = out_df[final_cols]
    out_df.to_csv(output_path, index=False)

    # ── summary stats ──
    print(f"\n✅  Done — {len(out_df)} enriched companies written to:\n    {output_path}")
    print("\n── Confidence Band Distribution ──")
    band_counts = out_df["match_confidence_band"].value_counts()
    for band, count in band_counts.items():
        pct = count / len(out_df) * 100
        bar = "█" * int(pct / 2)
        print(f"  {band:<8} {count:>4} ({pct:5.1f}%)  {bar}")

    print(f"\n── Score Stats ──")
    sc = out_df["match_confidence_score"]
    print(f"  Mean   : {sc.mean():.1f}")
    print(f"  Median : {sc.median():.1f}")
    print(f"  Min    : {sc.min():.1f}")
    print(f"  Max    : {sc.max():.1f}")

    print(f"\n── Match Flags ──")
    flag_rows = out_df[out_df["match_flags"] != "OK"]
    print(f"  Rows with at least one flag : {len(flag_rows)} ({len(flag_rows)/len(out_df)*100:.1f}%)")
    all_flags = []
    for f in out_df["match_flags"]:
        all_flags.extend(f.split("|"))
    from collections import Counter
    for flag, cnt in Counter(all_flags).most_common():
        if flag != "OK":
            print(f"    {flag:<25} {cnt}")


if __name__ == "__main__":
    INPUT  = "presales_data_sample - presales_data_sample.csv"
    OUTPUT = "enriched_companies.csv"

    run_pipeline(INPUT, OUTPUT)