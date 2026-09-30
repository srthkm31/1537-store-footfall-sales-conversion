# Store Footfall vs Sales Conversion Analytics

## Capstone Project

**Name:** Sarthak Mondal  
**Roll Number:** 1537  
**Batch:** DATABRICKS AND SNOWFLAKE 2026  
**Problem Statement:** Store Footfall vs Sales Conversion  
**Tools:** Databricks, PySpark, Delta Lake, Snowflake, SQL, GitHub

---

## 1. Project Overview

This project analyzes the relationship between **store footfall and sales conversion** using a complete data engineering pipeline built with Databricks and Snowflake.

The pipeline generates store-level hourly data, introduces realistic data-quality issues, processes the data through Bronze, Silver, and Gold layers, exports the final analytical dataset to Snowflake, and performs SQL-based business analysis.

The project demonstrates:

- Data generation
- Data ingestion
- Data quality handling
- Bronze/Silver/Gold architecture
- PySpark transformations
- Delta Lake tables
- Data validation
- CSV export
- Snowflake staging and loading
- SQL analytics
- Databricks Jobs
- GitHub-based project version control

---

## 2. Problem Statement

Retail stores generate large amounts of customer footfall and transaction data.

However, raw operational data can contain:

- Invalid sensor readings
- Offline footfall sensors
- Duplicate bills
- Unknown stores
- Transactions outside trading hours

The objective of this project is to build a reliable analytical pipeline that cleans these issues and produces a trusted store-hour dataset for calculating **sales conversion rate**.

### Conversion Rate

Conversion rate is calculated as:

```text
Conversion Rate = Bills / Footfall × 100
