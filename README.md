# MovieLens ETL Pipeline with OMDb Enrichment

This project implements a complete **ETL pipeline** using Python, Pandas, Requests, and SQLite.  
It loads the MovieLens dataset, cleans it, enriches it with OMDb API metadata, and stores the
final processed data into a SQLite database.

This project was completed as part of a **Data Engineering Assignment**.

---

## 📌 Project Overview

The ETL pipeline performs the following steps:

### **1. Extract**
- Reads `movies_small.csv` and `ratings_small.csv`.
- These smaller datasets were used for faster processing during development.

### **2. Transform**
- Cleans movie titles and extracts release year.
- Parses genres and organizes them for database insertion.
- Fetches extra movie information from **OMDb API**:
  - Director  
  - Plot  
  - Box Office  
  - Runtime  
  - Language  
  - Country  
- Uses a **local JSON cache** to avoid repeated API calls.

### **3. Load**
- Inserts the following into a SQLite database (`movies.db`):
  - Movies  
  - Genres  
  - Movie–Genre relationships  
  - Ratings  
- The project uses a normalized schema defined in `schema.sql`.

---

## 📂 Project Files

| File | Description |
|------|-------------|
| `etl.py` | Main ETL pipeline script |
| `schema.sql` | SQL script to create database schema |
| `queries.sql` | Contains all SQL queries for analysis |
| `movies_small.csv` | Small movies dataset |
| `ratings_small.csv` | Small ratings dataset |
| `omdb_key.txt` | Stores OMDb API key |
| `movies.db` | Final SQLite database (generated after ETL) |
| `README.md` | Project documentation |

---

## 🗂 Database Schema

The following tables are created:

### **1. movies**
Stores movie information (title, year, director, plot, etc.)

### **2. genres**
Stores unique genre names.

### **3. movie_genres**
Maps each movie to one or more genres.

### **4. ratings**
Stores users' ratings for movies.

---

## ▶️ How to Run the Project

### **1. Install dependencies**
```
pip install pandas requests
```

### **2. Place your OMDb API key in:**
```
omdb_key.txt
```

### **3. Run the ETL script**
```
python etl.py
```

After running, the output SQLite database will be generated as:
```
movies.db
```

---

## 📊 SQL Queries (Business Questions)

All SQL queries are in `queries.sql`.

### ✔ **1. Highest average rated movie**
Returns movieId, title, average rating, and count.

### ✔ **2. Top 5 genres with highest average rating**

### ✔ **3. Director with the most movies**

### ✔ **4. Average movie rating per year**

You can run these queries using any SQLite viewer (DB Browser for SQLite, DBeaver, etc.).

---

## 🛠 Tech Stack Used

- **Python 3.11**
- **Pandas**
- **Requests**
- **SQLite**
- **OMDb API**
- **PowerShell / Terminal**

---

## ✅ Final Notes

- Smaller datasets (`movies_small.csv`, `ratings_small.csv`) were used to reduce processing time.
- The ETL script includes a **progress bar**.
- A **JSON cache** is used to prevent repeated OMDb API calls.
- Final processed data is saved in `movies.db`.

---

## 🎉 Project Completed Successfully

This project demonstrates a complete ETL workflow with data cleaning, enrichment, and SQL analysis.  
It is ready for submission.

