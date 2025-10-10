# data_pipeline.py
# Author: Yucheng Lu
# Date: 2025-10-10
# Dependencies: pandas, sqlite3

import sqlite3
import pandas as pd
from datetime import datetime, timezone


def load_to_sqlite(comment_path, db_path='project_test.db'):
    """
    Load the comments CSV file into a SQLite database.

    Parameters:
        comment_path (str): Path to the comments CSV file.
        db_path (str): Path to the SQLite database file.

    Returns:
        sqlite3.Connection: Connection object to the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_csv(comment_path, low_memory=False)
    df.to_sql('comment_table', conn, if_exists='replace', index=False)
    return conn


def count_posts_comments(conn):
    """
    Count the number of unique posts and comments in the database.

    Parameters:
        conn (sqlite3.Connection): Active SQLite connection.

    Returns:
        tuple: (post_count, comment_count)
    """
    post_number = pd.read_sql_query(
        "SELECT COUNT(DISTINCT parent_id) AS post_count FROM comment_table", conn)
    comment_number = pd.read_sql_query(
        "SELECT COUNT(DISTINCT comment_id) AS comment_count FROM comment_table", conn)

    print(post_number)
    print(comment_number)
    return post_number.iloc[0, 0], comment_number.iloc[0, 0]


def clean_comments(comment_path):
    """
    Clean the comments dataset:
    - Drop unnecessary flair columns.
    - Convert UNIX timestamps to datetime.
    - Add a 'year' column for analysis.

    Parameters:
        comment_path (str): Path to the comments CSV file.

    Returns:
        pandas.DataFrame: Cleaned comments DataFrame.
    """
    comments = pd.read_csv(comment_path, low_memory=False)

    # Drop unused flair columns (ignore if missing)
    comments = comments.drop(columns=[
        'author_flair_text', 'author_flair_type',
        'author_flair_template_id', 'author_flair_richtext'
    ], errors='ignore')

    # Convert timestamp to datetime (UTC)
    comments['created_time'] = comments['created_utc'].apply(
        lambda x: datetime.fromtimestamp(x, tz=timezone.utc)
    )

    # Extract year
    comments['year'] = comments['created_time'].dt.year

    # Print time range
    earliest = comments['created_time'].min()
    latest = comments['created_time'].max()
    print(f"Earliest time = {earliest}")
    print(f"Latest time = {latest}")

    return comments


def merge_submissions_comments(sub_path, comment_path, output_path):
    """
    Merge submissions and comments datasets.

    Parameters:
        sub_path (str): Path to the submissions CSV file.
        comment_path (str): Path to the comments CSV file.
        output_path (str): File path to save the merged CSV.

    Returns:
        pandas.DataFrame: Merged DataFrame.
    """
    submissions = pd.read_csv(sub_path, low_memory=False)
    comments = clean_comments(comment_path)

    # Remove the "t1_" or "t3_" prefix from parent_id
    comments["parent_clean_id"] = comments["parent_id"].str.replace(
        r"t\\d+_", "", regex=True
    )

    # Merge on submission_id and parent_clean_id
    merged = pd.merge(
        submissions,
        comments,
        how="outer",
        left_on="submission_id",
        right_on="parent_clean_id",
        indicator=True
    )

    # Drop unnecessary flair columns from merged data
    merged = merged.drop(columns=[
        'link_flair_text', 'link_flair_type',
        'link_flair_template_id', 'link_flair_richtext'
    ], errors='ignore')

    # Save to CSV
    merged.to_csv(output_path, index=False)
    print(f"Merged data saved to {output_path}")
    print(merged.info())

    return merged


def main():
    """
    Main pipeline execution:
    1. Load data into SQLite.
    2. Count distinct posts and comments.
    3. Merge comments and submissions.
    """
    comment_csv = "../data/PoliticalDiscussion_comments_sample.csv"
    submission_csv = "../data/PoliticalDiscussion_submissions_sample.csv"
    output_csv = "../data/joined_data.csv"

    # Step 1: Load comments into SQLite and count
    conn = load_to_sqlite(comment_csv)
    count_posts_comments(conn)

    # Step 2: Merge datasets and save output
    merge_submissions_comments(submission_csv, comment_csv, output_csv)


if __name__ == "__main__":
    main()
