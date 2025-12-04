"""
etl.py
Simple ETL for MovieLens (small) + OMDb enrichment -> SQLite
Very beginner-friendly, uses pandas + requests + sqlite3
Creates an 'movies.db' SQLite file in the repo folder.
"""

import os
import json
import time
import sqlite3
from pathlib import Path
import pandas as pd
import requests

MOVIES_CSV = "movies_small.csv"    # using smaller datasets
RATINGS_CSV = "ratings_small.csv"
DB_FILE = "movies.db"
OMDB_CACHE = "omdb_cache.json"
OMDB_KEY_FILE = "omdb_key.txt"
OMDB_URL = "http://www.omdbapi.com/"

SLEEP_BETWEEN_CALLS = 0.2  # OMDb limit


# ------------------------------------------------------------
# PROGRESS BAR
# ------------------------------------------------------------

def progress_bar(current, total, prefix=""):
    percent = (current / total) * 100
    bar_len = 40
    filled = int(bar_len * current // total)
    bar = "#" * filled + "-" * (bar_len - filled)
    print(f"\r{prefix} [{bar}] {percent:5.1f}% ({current}/{total})", end="", flush=True)


# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def load_omdb_key():
    if os.path.exists(OMDB_KEY_FILE):
        return Path(OMDB_KEY_FILE).read_text().strip()
    return None


def load_cache():
    if os.path.exists(OMDB_CACHE):
        try:
            with open(OMDB_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_cache(cache):
    with open(OMDB_CACHE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


# ------------------------------------------------------------
# Database initialization
# ------------------------------------------------------------

def init_db(conn):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    schema_sql = Path("schema.sql").read_text()
    cur.executescript(schema_sql)
    conn.commit()


# ------------------------------------------------------------
# ETL Steps
# ------------------------------------------------------------

def extract():
    print("Loading movies.csv ...")
    movies = pd.read_csv(MOVIES_CSV)
    print("Loaded movies:", len(movies))

    print("Loading ratings.csv ...")
    ratings = pd.read_csv(RATINGS_CSV)
    print("Loaded ratings:", len(ratings))

    # keep only valid ratings (FK-safe)
    valid_ids = set(movies["movieId"])
    ratings = ratings[ratings["movieId"].isin(valid_ids)]
    print("Ratings after filtering:", len(ratings))

    return movies, ratings



def clean_title_and_year(raw_title):
    if raw_title.endswith(")") and "(" in raw_title:
        try:
            idx = raw_title.rfind("(")
            title = raw_title[:idx].strip()
            year = int(raw_title[idx+1:-1])
            return title, year
        except:
            return raw_title, None
    return raw_title, None


def fetch_omdb(title, year, api_key, cache):
    key = f"{title.lower()}_{year}".strip()

    if key in cache:
        return cache[key]

    if not api_key:
        return {"Response": "False"}

    params = {"apikey": api_key, "t": title}
    if year:
        params["y"] = str(year)

    try:
        response = requests.get(OMDB_URL, params=params, timeout=10)
        data = response.json()
    except:
        data = {"Response": "False"}

    cache[key] = data
    time.sleep(SLEEP_BETWEEN_CALLS)
    return data


def transform_and_load(movies_df, ratings_df, conn, api_key):
    cache = load_cache()
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM genres")
    genre_map = {name: gid for gid, name in cur.fetchall()}

    # ------------------------------------------------------------
    # MOVIES LOOP (with progress bar)
    # ------------------------------------------------------------
    total_movies = len(movies_df)
    print("\nProcessing movies...")

    for idx, (_, row) in enumerate(movies_df.iterrows(), start=1):
        progress_bar(idx, total_movies, prefix="Movies")

        movieId = int(row["movieId"])
        raw_title = row["title"]
        genres_str = row["genres"]

        title, year = clean_title_and_year(raw_title)

        omdb = fetch_omdb(title, year, api_key, cache)

        imdb_id = omdb.get("imdbID") if omdb.get("Response") == "True" else None
        director = omdb.get("Director") if omdb.get("Response") == "True" else None
        plot = omdb.get("Plot") if omdb.get("Response") == "True" else None
        box_office = omdb.get("BoxOffice") if omdb.get("Response") == "True" else None
        runtime = omdb.get("Runtime") if omdb.get("Response") == "True" else None
        language = omdb.get("Language") if omdb.get("Response") == "True" else None
        country = omdb.get("Country") if omdb.get("Response") == "True" else None

        cur.execute("""
            INSERT OR REPLACE INTO movies 
            (movieId, title, year, imdb_id, director, plot, box_office, runtime, language, country)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (movieId, title, year, imdb_id, director, plot, box_office, runtime, language, country))
        conn.commit()

        if isinstance(genres_str, str):
            genres = [g for g in genres_str.split("|") if g != "(no genres listed)"]
        else:
            genres = []

        for g in genres:
            if g not in genre_map:
                cur.execute("INSERT OR IGNORE INTO genres (name) VALUES (?)", (g,))
                conn.commit()
                cur.execute("SELECT id FROM genres WHERE name=?", (g,))
                gid = cur.fetchone()[0]
                genre_map[g] = gid
            else:
                gid = genre_map[g]

            cur.execute("""
                INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                VALUES (?, ?)
            """, (movieId, gid))
            conn.commit()

    print("\nMovies processing completed.\n")
    save_cache(cache)

    # ------------------------------------------------------------
    # RATINGS LOOP (with progress bar)
    # ------------------------------------------------------------
    total_ratings = len(ratings_df)
    print("Processing ratings...")

    cur.execute("BEGIN")

    for idx, (_, row) in enumerate(ratings_df.iterrows(), start=1):
        progress_bar(idx, total_ratings, prefix="Ratings")

        cur.execute("""
            INSERT OR IGNORE INTO ratings (userId, movieId, rating, timestamp)
            VALUES (?, ?, ?, ?)
        """, (int(row["userId"]), int(row["movieId"]),
              float(row["rating"]), int(row["timestamp"])))

    conn.commit()
    print("\nRatings processing completed.\n")

    print("ETL completed successfully.")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    print("Starting ETL...\n")

    api_key = load_omdb_key()
    if not api_key:
        print("No OMDb API key found. Enrichment will be skipped.")

    movies_df, ratings_df = extract()

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    transform_and_load(movies_df, ratings_df, conn, api_key)
    conn.close()

    print("Database created:", DB_FILE)


if __name__ == "__main__":
    main()
