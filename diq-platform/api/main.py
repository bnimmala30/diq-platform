from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
import anthropic
import os
from datetime import datetime
import json

# ── App Setup ─────────────────────────────────────────────────
app = FastAPI(
    title       = "DataIQ360 API",
    description = "AI-powered retail analytics for Australian shops",
    version     = "1.0.0"
)

# ── CORS (allows dashboard to call API) ───────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_credentials = True,
    allow_methods     = ["*"],
    allow_headers     = ["*"],
)

# ── Snowflake Config ──────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    'account':   'UQPZXAN-AI20794',
    'user':      'bhavani30',
    'password':  'Janyajoshya@14',
    'warehouse': 'diq_warehouse',
    'database':  'dataiq360_db',
    'role':      'diq_admin_role'
}

# ── Snowflake Connection ──────────────────────────────────────
def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def run_query(query):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0].lower() for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

# ── Routes ────────────────────────────────────────────────────

@app.get("/")
def home():
    return {
        "product": "DataIQ360",
        "version": "1.0.0",
        "status":  "running",
        "message": "AI-powered retail analytics for Australian shops"
    }

@app.get("/health")
def health():
    return {
        "status":    "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/shops")
def get_shops():
    query = """
        SELECT
            shop_id,
            shop_name,
            city,
            total_revenue,
            total_transactions,
            avg_transaction_value,
            unique_customers,
            revenue_per_customer,
            weekend_revenue,
            weekday_revenue,
            card_payment_pct,
            revenue_rank,
            pct_of_total_revenue
        FROM dataiq360_db.raw_marts.shop_performance
        ORDER BY revenue_rank
    """
    data = run_query(query)
    return {
        "success": True,
        "count":   len(data),
        "shops":   data
    }

@app.get("/shops/{shop_id}")
def get_shop(shop_id: str):
    query = f"""
        SELECT *
        FROM dataiq360_db.raw_marts.shop_performance
        WHERE shop_id = '{shop_id.upper()}'
    """
    data = run_query(query)
    if not data:
        raise HTTPException(
            status_code = 404,
            detail      = f"Shop {shop_id} not found"
        )
    return {
        "success": True,
        "shop":    data[0]
    }

@app.get("/revenue/daily")
def get_daily_revenue():
    query = """
        SELECT
            transaction_date,
            shop_id,
            shop_name,
            daily_revenue,
            total_transactions,
            avg_transaction_value,
            day_of_week,
            is_weekend
        FROM dataiq360_db.raw_analytics.daily_revenue
        ORDER BY transaction_date DESC
        LIMIT 100
    """
    data = run_query(query)
    return {
        "success": True,
        "count":   len(data),
        "data":    data
    }

@app.get("/revenue/by-day")
def get_revenue_by_day():
    query = """
        SELECT
            day_of_week,
            ROUND(AVG(daily_revenue), 2)  AS avg_revenue,
            ROUND(SUM(daily_revenue), 2)  AS total_revenue,
            SUM(total_transactions)        AS total_transactions
        FROM dataiq360_db.raw_analytics.daily_revenue
        GROUP BY day_of_week
        ORDER BY avg_revenue DESC
    """
    data = run_query(query)
    return {
        "success": True,
        "data":    data
    }

@app.get("/products")
def get_top_products():
    query = """
        SELECT
            shop_name,
            product_name,
            category,
            total_revenue,
            total_units_sold,
            avg_sale_value,
            revenue_rank_in_shop,
            pct_of_shop_revenue
        FROM dataiq360_db.raw_analytics.top_products
        WHERE revenue_rank_in_shop <= 5
        ORDER BY shop_name, revenue_rank_in_shop
    """
    data = run_query(query)
    return {
        "success": True,
        "count":   len(data),
        "products": data
    }

@app.get("/insights")
def get_insights():
    try:
        shops    = run_query("SELECT shop_name, city, total_revenue, avg_transaction_value, revenue_rank, pct_of_total_revenue FROM dataiq360_db.raw_marts.shop_performance ORDER BY revenue_rank")
        products = run_query("SELECT shop_name, product_name, total_revenue, revenue_rank_in_shop FROM dataiq360_db.raw_analytics.top_products WHERE revenue_rank_in_shop <= 3 ORDER BY shop_name")
        daily    = run_query("SELECT day_of_week, ROUND(AVG(daily_revenue),2) AS avg_revenue FROM dataiq360_db.raw_analytics.daily_revenue GROUP BY day_of_week ORDER BY avg_revenue DESC")

        client = anthropic.Anthropic(
            api_key=os.environ.get('ANTHROPIC_API_KEY')
        )

        prompt = f"""
You are DataIQ360 AI advisor for Australian retail shops.
Analyse this data and give 3 specific plain English insights.
Be brief. Use dollar amounts. Maximum 200 words total.

Shop Performance: {json.dumps(shops, default=str)}
Top Products: {json.dumps(products, default=str)}
Best Days: {json.dumps(daily, default=str)}

Give exactly 3 insights numbered 1, 2, 3.
"""
        message = client.messages.create(
            model      = "claude-opus-4-5",
            max_tokens = 300,
            messages   = [{"role": "user", "content": prompt}]
        )

        return {
            "success":  True,
            "insights": message.content[0].text,
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/summary")
def get_summary():
    query = """
        SELECT
            COUNT(*)                       AS total_shops,
            ROUND(SUM(total_revenue), 2)   AS total_revenue,
            SUM(total_transactions)         AS total_transactions,
            ROUND(AVG(avg_transaction_value), 2) AS avg_transaction_value,
            SUM(unique_customers)           AS total_customers
        FROM dataiq360_db.raw_marts.shop_performance
    """
    data = run_query(query)
    return {
        "success": True,
        "summary": data[0]
    }
