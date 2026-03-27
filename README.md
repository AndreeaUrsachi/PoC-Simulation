# PoC-Simulation

## Overview

This challenge involved a CSV file containing data for multiple companies, where **each company had 5 candidate rows**. The goal was to identify the correct row for each company and produce a clean, consistent dataset.

---

## Approach

### 1. Research — Finding Reliable Data Sources

The first step was researching which sources could be used to validate company data. The most promising option was **Dun & Bradstreet**, for which an API key was requested — unfortunately, access was not granted in time.

As an alternative, a **web scraping solution** was built: given a company name, the script searches the web, aggregates what it finds, computes a **match score**, and selects the most likely correct row among the 5 candidates.

### 2. Manual Verification

To ensure accuracy, results were cross-checked manually using:

- **Crunchbase** — company profiles and funding data
- **OpenCorporates** — official company registration data
- **Google** — general search for public company information
- **Company websites** — where available

### 3. Data Cleaning

After identifying the correct rows, the data required significant cleaning due to inconsistencies across fields:

| Issue | Fix Applied |
|---|---|
| Corrupted coordinates | Fixed dots used as thousands separators (e.g. `57.012.755` → `57.012755`) |
| Broken phone numbers | Converted floats to properly formatted strings (e.g. `4596356150.0` → `+4596356150`) |
| Float → integer fields | Year founded, employee count cast to integers |
| Revenue formatting | Raw integers formatted with thousand separators |
| Whitespace & casing | Stripped whitespace; categoricals title-cased; URLs lowercased |
| Empty cells | Standardised to `-` instead of blank/NaN |

### 4. Code Generation & Validation

**Claude** was used to generate the data verification and cleaning code. All outputs also passed a **manual review** step to confirm correctness before finalising.

---

## Challenges

- **Limited public data** — Some companies, for example those based in Pakistan, had little to no publicly available information, making match scoring and validation significantly harder.
- **Paywalled sources** — Several platforms that hold company data (including Crunchbase and OpenCorporates) require a premium account to access full records. Free tiers were used where possible.
- **D&B API access** — The ideal data source (Dun & Bradstreet) was not accessible within the project timeframe.

---

## Tools & Sources Used

| Tool | Purpose |
|---|---|
| Python + web scraping | Automated candidate row scoring |
| Claude | Code generation for data verification and cleaning |
| Crunchbase | Manual company validation |
| OpenCorporates | Official registration data |
| Google | General company research |
| Company websites | Direct source verification |

---

## Reflections

This was a genuinely interesting challenge — combining research, automation, and manual validation to turn messy, inconsistent data into something reliable. The entity resolution problem (picking the right row out of 5 candidates) turned out to be the hardest part, especially for less well-known companies with limited online presence.

Looking forward to presenting this.
