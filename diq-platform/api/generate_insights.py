import anthropic
import snowflake.connector
import os
import json
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    'account':   'UQPZXAN-AI20794',
    'user':      'bhavani30',
    'password':  'Janyajoshya@14',
    'warehouse': 'diq_warehouse',
    'database':  'dataiq360_db',
    'role':      'diq_admin_role'
}

ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')

# ── Connect to Snowflake ───────────────────────────────────────
def get_snowflake_data():
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # Get shop performance data
    cursor.execute("""
        SELECT
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
            high_value_transactions,
            medium_value_transactions,
            low_value_transactions,
            revenue_rank,
            pct_of_total_revenue
        FROM dataiq360_db.raw_marts.shop_performance
        ORDER BY revenue_rank
    """)
    shop_data = cursor.fetchall()
    shop_columns = [desc[0] for desc in cursor.description]

    # Get top products per shop
    cursor.execute("""
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
        WHERE revenue_rank_in_shop <= 3
        ORDER BY shop_name, revenue_rank_in_shop
    """)
    product_data = cursor.fetchall()
    product_columns = [desc[0] for desc in cursor.description]

    # Get daily revenue trends
    cursor.execute("""
        SELECT
            day_of_week,
            ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue,
            ROUND(SUM(daily_revenue), 2) AS total_revenue,
            SUM(total_transactions)       AS total_transactions
        FROM dataiq360_db.raw_analytics.daily_revenue
        GROUP BY day_of_week
        ORDER BY avg_daily_revenue DESC
    """)
    daily_data = cursor.fetchall()
    daily_columns = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return (
        [dict(zip(shop_columns, row)) for row in shop_data],
        [dict(zip(product_columns, row)) for row in product_data],
        [dict(zip(daily_columns, row)) for row in daily_data]
    )

# ── Generate AI Insights ───────────────────────────────────────
def generate_insights(shop_data, product_data, daily_data):
    print("Connecting to Claude AI...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""
You are DataIQ360 — an AI business advisor for Australian retail shops.

Analyse the following retail data and provide clear, actionable,
plain English business insights. Write as if speaking directly
to the shop owner. Be specific with numbers. Be encouraging but honest.

SHOP PERFORMANCE DATA:
{json.dumps(shop_data, indent=2, default=str)}

TOP 3 PRODUCTS PER SHOP:
{json.dumps(product_data, indent=2, default=str)}

REVENUE BY DAY OF WEEK:
{json.dumps(daily_data, indent=2, default=str)}

Please provide:
1. EXECUTIVE SUMMARY (3-4 sentences overview)
2. TOP PERFORMER ANALYSIS (which shop is winning and why)
3. IMPROVEMENT OPPORTUNITIES (which shop needs help and specific suggestions)
4. PRODUCT INSIGHTS (best and worst performing products)
5. TIMING INSIGHTS (best days and actionable staffing recommendations)
6. ONE BIG RECOMMENDATION (the single most impactful action to take)

Write in plain English. No jargon. Be specific with dollar amounts.
Format each section with a clear heading.
"""

    message = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=1500,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return message.content[0].text

# ── Save Insights ──────────────────────────────────────────────
def save_insights(insights):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"insights_{timestamp}.txt"

    with open(filename, 'w') as f:
        f.write(f"DataIQ360 AI Insights\n")
        f.write(f"Generated: {datetime.now().strftime('%d %B %Y %H:%M')}\n")
        f.write("="*60 + "\n\n")
        f.write(insights)

    print(f"Insights saved to: {filename}")
    return filename

# ── Main ───────────────────────────────────────────────────────
def main():
    print("="*60)
    print("DataIQ360 AI Insights Generator")
    print("="*60)

    # Get data from Snowflake
    shop_data, product_data, daily_data = get_snowflake_data()
    print(f"Loaded {len(shop_data)} shops from Snowflake ✅")
    print(f"Loaded {len(product_data)} top products ✅")
    print(f"Loaded {len(daily_data)} daily patterns ✅")

    # Generate insights with Claude AI
    print("\nGenerating AI insights...")
    insights = generate_insights(shop_data, product_data, daily_data)

    # Save to file
    filename = save_insights(insights)

    # Print to screen
    print("\n" + "="*60)
    print("AI INSIGHTS:")
    print("="*60)
    print(insights)
    print("="*60)
    print("\nDone! DataIQ360 insights generated successfully! ✅")

if __name__ == "__main__":
    main()
