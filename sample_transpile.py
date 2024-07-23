from sqlglot import parse, transpile
from sqlglot.dialects.e6data import E6data  # Ensure this import matches your file structure

# Snowflake Query
snowflake_query = """
WITH user_actions AS (
    SELECT user_id, action_type, COUNT(*) as action_count
    FROM event_log
    WHERE DATE(event_timestamp) BETWEEN DATEADD(day, -30, CURRENT_DATE()) AND CURRENT_DATE()
    GROUP BY user_id, action_type
)
SELECT 
    u.user_id,
    u.email,
    ua.action_type,
    ua.action_count,
    RANK() OVER (PARTITION BY ua.action_type ORDER BY ua.action_count DESC) as action_rank
FROM 
    users u
    JOIN user_actions ua ON u.user_id = ua.user_id
WHERE 
    u.is_active = TRUE
    AND ua.action_count > 10
QUALIFY action_rank <= 5
ORDER BY 
    ua.action_type, 
    action_rank
"""

# Trino Query
trino_query = """
WITH daily_sales AS (
    SELECT 
        DATE_TRUNC('day', order_date) AS sale_date,
        SUM(total_amount) AS daily_total
    FROM 
        orders
    WHERE 
        order_date >= DATE '2023-01-01'
    GROUP BY 
        DATE_TRUNC('day', order_date)
)
SELECT 
    sale_date,
    daily_total,
    AVG(daily_total) OVER (
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7day
FROM 
    daily_sales
WHERE 
    daily_total > 1000
ORDER BY 
    sale_date DESC
LIMIT 100
"""

# Spark Query
spark_query = """
SELECT 
    product_id,
    product_name,
    category,
    price,
    CASE 
        WHEN price < 50 THEN 'Budget'
        WHEN price BETWEEN 50 AND 200 THEN 'Mid-range'
        ELSE 'Premium'
    END AS price_category,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as price_rank_in_category
FROM 
    products
WHERE 
    is_available = true
    AND last_updated_date > date_sub(current_date(), 90)
HAVING 
    price_rank_in_category <= 5
"""

# Convert queries to E6data dialect
def convert_to_e6data(query, from_dialect):
    parsed = parse(query, read=from_dialect)
    if isinstance(parsed, list):
        # If multiple statements, convert each one
        e6data_sql = [expr.sql(dialect="e6data") for expr in parsed]
        return "\n".join(e6data_sql)
    else:
        # If single statement
        return parsed.sql(dialect="e6data")

print("Snowflake to E6data:")
print(convert_to_e6data(snowflake_query, "snowflake"))
print("\n" + "="*50 + "\n")

print("Trino to E6data:")
print(convert_to_e6data(trino_query, "trino"))
print("\n" + "="*50 + "\n")

print("Spark to E6data:")
print(convert_to_e6data(spark_query, "spark"))