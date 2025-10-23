import os
import pandas as pd
from datetime import datetime
from tqdm import tqdm

submissions_folder = r"C:\Users\Nika\Documents\Computational Social Science\Project\FilteredCSVs\subs"
comments_folder = r"C:\Users\Nika\Documents\Computational Social Science\Project\FilteredCSVs\coms"
output_file = "normalizedFINALdata.csv"


def normalize_submissions(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["created"], format="%Y-%m-%d", errors="coerce")
    df["title"] = df["title"].fillna("")
    df["body"] = df["selftext_or_url"].fillna("")
    df["url"] = "https://www.reddit.com" + df["permalink"].astype(str)
    df["subreddit"] = df["permalink"].str.extract(r"/r/([^/]+)/").fillna("unknown")
    df["author"] = df["author"].fillna("[deleted]")
    df["is_submission"] = True
    cols = ["score", "title", "body", "url", "date", "subreddit", "author", "is_submission"]
    return df[cols]


def normalize_comments(df):
    df = df.copy()
    df["date"] = pd.to_datetime(df["created_utc"], format="%Y-%m-%d", errors="coerce")
    df["title"] = (
        df["permalink"]
        .str.extract(r"/comments/[^/]+/([^/]+)/")
        .fillna("")
        .astype(str)
        .apply(lambda s: s.replace("_", " "))
    )
    df["body"] = df["body"].fillna("")
    df["url"] = "https://www.reddit.com" + df["permalink"].astype(str)
    df["subreddit"] = df["subreddit"].fillna("unknown")
    df["author"] = df["author"].fillna("[deleted]")
    df["is_submission"] = False
    cols = ["score", "title", "body", "url", "date", "subreddit", "author", "is_submission"]
    return df[cols]


def process_folder(folder_path, mode):
    if not os.path.exists(folder_path):
        return pd.DataFrame(columns=[
            "score", "title", "body", "url", "date", "subreddit", "author", "is_submission"
        ])

    csv_files = [
        os.path.join(folder_path, f)
        for f in os.listdir(folder_path)
        if f.lower().endswith(".csv")
    ]
    if not csv_files:
        return pd.DataFrame(columns=[
            "score", "title", "body", "url", "date", "subreddit", "author", "is_submission"
        ])

    parts = []
    for file in tqdm(csv_files, desc=f"{mode}", ncols=80):
        try:
            df = pd.read_csv(file, low_memory=False)
        except Exception:
            continue
        if mode == "submissions" and "selftext_or_url" in df.columns:
            part = normalize_submissions(df)
        elif mode == "comments" and "body" in df.columns:
            part = normalize_comments(df)
        else:
            continue
        parts.append(part)

    if not parts:
        return pd.DataFrame(columns=[
            "score", "title", "body", "url", "date", "subreddit", "author", "is_submission"
        ])

    return pd.concat(parts, ignore_index=True)


if __name__ == "__main__":
    subs_df = process_folder(submissions_folder, "submissions")
    coms_df = process_folder(comments_folder, "comments")

    merged = pd.concat([subs_df, coms_df], ignore_index=True)
    before = len(merged)
    merged = merged.drop_duplicates(subset=["title", "body", "url"])
    after = len(merged)
    removed = before - after

    merged = merged.sort_values(by="date", ascending=True)
    merged.to_csv(output_file, index=False)

    total = len(merged)
    subs = merged["is_submission"].sum()
    coms = total - subs
    print(f"Submissions: {subs}, Comments: {coms}, Total: {total}, Duplicates removed: {removed}")
