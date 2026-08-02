"""
Post-scrape validation/quality-check for korea_herald_corpus.csv.

Run this after scrape_korea_herald.py finishes (or partway through, to
sanity-check a partial run). Produces:
    - console report: totals, failure breakdown, per-year distribution
    - review_short_articles.csv: rows with body_text under 150 words,
      flagged as possible extraction failures
    - a random sample of 10 rows printed to console for eyeballing quality
"""

import random

import pandas as pd

CORPUS_CSV = "korea_herald_corpus.csv"
FAILURES_CSV = "scrape_failures.csv"
REVIEW_CSV = "review_short_articles.csv"
SHORT_ARTICLE_WORD_THRESHOLD = 150


def main():
    df = pd.read_csv(CORPUS_CSV, dtype={"article_id": int}, keep_default_na=False)

    total = len(df)
    succeeded = (df["scrape_status"] == "success").sum()
    failed = (df["scrape_status"] == "failed").sum()

    print("=" * 70)
    print("SCRAPE SUMMARY")
    print("=" * 70)
    print(f"Total attempted: {total}")
    print(f"Succeeded:       {succeeded}")
    print(f"Failed:          {failed}")

    print()
    print("Failure breakdown by error_type:")
    try:
        fdf = pd.read_csv(FAILURES_CSV, keep_default_na=False)
        if len(fdf):
            print(fdf["error_type"].value_counts().to_string())
        else:
            print("  (no failures logged)")
    except FileNotFoundError:
        print(f"  ({FAILURES_CSV} not found)")

    print()
    print("Rows with an unparsed/low-confidence publication date (date_flag):")
    if "date_flag" in df.columns:
        flagged = df[df["date_flag"] != ""]
        print(f"  {len(flagged)} rows flagged")
        if len(flagged):
            print(flagged["date_flag"].value_counts().to_string())

    print()
    print("Distribution of articles per year (successful rows with a parsed date):")
    success_df = df[df["scrape_status"] == "success"].copy()
    dated = success_df[success_df["publication_date"] != ""].copy()
    dated["year"] = pd.to_datetime(dated["publication_date"], errors="coerce").dt.year
    year_counts = dated["year"].value_counts().sort_index()
    print(year_counts.to_string())
    n_no_date = len(success_df) - len(dated)
    print(f"(successful rows with no usable publication_date: {n_no_date})")

    print()
    print(f"Flagging articles with body_text under {SHORT_ARTICLE_WORD_THRESHOLD} words "
          f"as possible extraction failures...")
    word_counts = success_df["body_text"].astype(str).str.split().str.len()
    short_articles = success_df[word_counts < SHORT_ARTICLE_WORD_THRESHOLD]
    short_articles.to_csv(REVIEW_CSV, index=False)
    print(f"  {len(short_articles)} short articles written to {REVIEW_CSV}")

    print()
    print("=" * 70)
    print("RANDOM SAMPLE OF 10 SUCCESSFUL ROWS (title, date, first 200 chars of body)")
    print("=" * 70)
    sample_pool = success_df[success_df["body_text"].astype(str).str.len() > 0]
    sample_n = min(10, len(sample_pool))
    if sample_n == 0:
        print("No successful rows with body text to sample.")
    else:
        sample = sample_pool.sample(n=sample_n, random_state=random.randint(0, 10_000))
        for _, row in sample.iterrows():
            print("-" * 70)
            print(f"[{row['article_id']}] {row['title']}")
            print(f"  date: {row['publication_date'] or '(none)'}  "
                  f"method: {row.get('extraction_method', '')}")
            body = str(row["body_text"])
            print(f"  body: {body[:200]!r}")


if __name__ == "__main__":
    main()
