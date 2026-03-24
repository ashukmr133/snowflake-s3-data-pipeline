# Snowflake S3 Data Pipeline 🚀

## 📌 Project Overview

This project demonstrates an end-to-end data pipeline using Snowflake and AWS S3.

## 🧱 Architecture

S3 → Snowflake Stage → Raw Table → Clean Transform → Final Table

## ⚙️ Features

* External Stage using S3
* Storage Integration (IAM Role-based access)
* Handling messy CSV data
* Data cleaning using SQL
* Deduplication using ROW_NUMBER
* Metadata tracking (file name, load time, user)
* Duplicate file prevention using `FORCE = FALSE`

## 📂 Data Issues Handled

* NULL values (NULL, N/A, empty)
* Invalid numeric values
* Extra spaces
* Cities with country names
* Duplicate records

## 🛠️ Tech Stack

* Snowflake
* AWS S3
* SQL

## 🚀 How to Run

1. Create file format
2. Create storage integration
3. Create stage
4. Create tables
5. Run COPY INTO
6. Run transformation queries

## 📸 Future Improvements

* MERGE (Upsert logic)
* SCD Type 2 implementation
* Automated pipeline using Tasks

---

⭐ If you like this project, give it a star!
