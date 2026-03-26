# DataIQ360 — AI-Powered Retail Analytics

> AI-powered retail analytics platform for Australian small businesses.
> Built with AWS · Snowflake · dbt · Claude AI · FastAPI · HTML Dashboard

![Status](https://img.shields.io/badge/Status-Live-brightgreen)
![AWS](https://img.shields.io/badge/Cloud-AWS-orange)
![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-blue)
![Claude AI](https://img.shields.io/badge/AI-Claude-purple)
![FastAPI](https://img.shields.io/badge/API-FastAPI-green)

---

## What Is DataIQ360?

DataIQ360 is a complete end-to-end data engineering platform that:

- Automatically collects retail transaction data
- Cleans and transforms it using AWS Glue PySpark ETL
- Loads it into Snowflake data warehouse
- Transforms it with dbt SQL models
- Analyses it using Claude AI (Anthropic)
- Serves insights via a FastAPI REST API
- Displays everything on a live dashboard

**Live Demo:** http://54.79.188.174:8000/dashboard

---

## The Problem It Solves

Small Australian retail shops earning $500K-$10M per year have no affordable analytics solution:

- **Too expensive:** Tableau costs $70/user/month
- **Too complex:** Requires a data analyst to operate
- **No AI insights:** Just charts — no plain English advice

**DataIQ360 fills this gap at $99/month** — fully automated, zero expertise needed.

---

## Architecture — 8 Layer Pipeline
Raw CSV in S3
|
v
AWS Glue PySpark ETL  (clean + enrich)
|
v
S3 Processed (Parquet)
|
v
Snowflake Data Warehouse  (12,482 rows)
|
v
dbt SQL Models  (4 models)
|
v
Claude AI Insights  (plain English advice)
|
v
FastAPI REST API  (9 endpoints)
|
v
Live Dashboard  (charts + AI insights)

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Infrastructure | AWS EC2 + S3 + IAM | Server + Storage + Security |
| ETL | AWS Glue + PySpark | Data cleaning pipeline |
| Warehouse | Snowflake | Cloud data warehouse |
| Transform | dbt | SQL model transformations |
| AI | Claude API (Anthropic) | Plain English insights |
| Backend | FastAPI + Uvicorn | REST API server |
| Frontend | HTML + CSS + JS + Chart.js | Live dashboard |
| Version Control | Git + GitHub | Code management |

---

## Project Structure
diq-platform/
├── data_generator/          # Python synthetic data generator
│   └── generate_data.py     # Creates 12,482 retail transactions
├── glue_jobs/               # AWS Glue PySpark ETL scripts
│   └── diq_raw_to_processed.py
├── snowflake/               # Snowflake setup SQL
│   └── setup.sql
├── diq_transform/           # dbt transformation project
│   └── models/
│       ├── staging/         # stg_transactions (view)
│       ├── analytics/       # daily_revenue, top_products (tables)
│       └── marts/           # shop_performance (table)
├── api/                     # FastAPI backend
│   ├── main.py              # 9 REST endpoints
│   ├── generate_insights.py # Claude AI script
│   └── index.html           # Live dashboard
└── README.md

---

## Data Pipeline Details

### Synthetic Data
- 12,482 transactions across 5 Australian retail shops
- Realistic patterns: Saturday +40%, Christmas +80%, seasonal trends
- Generated using Python faker library with Australian locale

### AWS Glue ETL
- Reads raw CSV files from S3
- Cleans: removes nulls, duplicates, impossible values
- Enriches: adds day_of_week, is_weekend, revenue_category, australian_season
- Writes: clean Parquet files to S3 (5x smaller, 10x faster)
- Runtime: 1 minute 40 seconds

### Snowflake + dbt
- Storage Integration for secure S3 connection (no credentials stored)
- RBAC with 4 roles (admin, transform, loader, reporting)
- 4 dbt models running in 6.43 seconds
- daily_revenue: 1,828 rows | top_products: 25 rows | shop_performance: 5 rows

### Claude AI Insights
- Reads Snowflake analytics tables via Python
- Sends structured retail data to Claude API
- Returns plain English business recommendations
- Example: "Perth Home Living dominates with $1.08M revenue (49% of total)"

---

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| GET / | API information |
| GET /health | Health check |
| GET /shops | All 5 shops with revenue data |
| GET /shops/{id} | Single shop details |
| GET /revenue/daily | Daily revenue trends |
| GET /revenue/by-day | Revenue by day of week |
| GET /products | Top products per shop |
| GET /insights | Claude AI business insights |
| GET /summary | Portfolio overview |
| GET /dashboard | Live HTML dashboard |
| GET /docs | Swagger API documentation |

---

## Key Results

- **Total Revenue:** $2,224,250 across 5 shops
- **Top Shop:** Perth Home Living — $1,083,456 (49% of total)
- **Best Day:** Saturday — $395,768 average weekly revenue
- **AI Insight:** "Weekends generate 34% of weekly revenue in just 2 days"

---

## AWS Infrastructure

- **Region:** ap-southeast-2 (Sydney) — Australian data sovereignty
- **EC2:** t3.micro — Amazon Linux 2023 — Session Manager (zero open ports)
- **Security:** IAM roles with least privilege — no SSH keys
- **S3:** diq-lakehouse-prod — raw/ processed/ analytics/ scripts/

---

## Skills Demonstrated

- AWS (EC2, S3, Glue, IAM, Session Manager)
- Snowflake data warehouse design and RBAC
- dbt SQL transformations with window functions
- PySpark ETL with AWS Glue
- Claude AI API integration and prompt engineering
- FastAPI REST API development
- HTML/CSS/JavaScript dashboard
- Git version control and professional commit history
- Linux server administration
- Data lake architecture (medallion pattern)

---

## Author

**bhavani30** — Building DataIQ360 from scratch as a portfolio project
to demonstrate real-world data engineering skills.

---

*Built with AWS Free Tier + Snowflake Trial + Anthropic API*
