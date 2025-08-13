from sqlalchemy import text
import pandas as pd

def create_table_if_not_exists(conn, table):
    conn.execute(text(f"""
    CREATE TABLE IF NOT EXISTS {table}(
        order_id      VARCHAR PRIMARY KEY,
        order_date    TIMESTAMP,
        customer_id   VARCHAR,
        product       VARCHAR,
        quantity      NUMERIC,
        unit_price    NUMERIC,
        amount        NUMERIC,
        city          VARCHAR,
        state         VARCHAR
    );
    """))

def upsert_dataframe(conn, df: pd.DataFrame, table: str, pk: str):
    temp = f"{table}_stg"
    # Clean staging table
    conn.execute(text(f"DROP TABLE IF EXISTS {temp};"))

    # Create temp staging table with same structure as target
    # (Postgres syntax; safer than SELECT * LIMIT 0)
    conn.execute(text(f"CREATE TEMP TABLE {temp} (LIKE {table} INCLUDING ALL);"))

    # IMPORTANT: pass the SQLAlchemy Connection (conn), NOT conn.connection
    df.to_sql(temp, con=conn, if_exists="append", index=False, method="multi")

    # Upsert from staging into target
    conn.execute(text(f"""
    INSERT INTO {table} AS t
      SELECT * FROM {temp}
    ON CONFLICT ({pk})
    DO UPDATE SET
      order_date = EXCLUDED.order_date,
      customer_id= EXCLUDED.customer_id,
      product    = EXCLUDED.product,
      quantity   = EXCLUDED.quantity,
      unit_price = EXCLUDED.unit_price,
      amount     = EXCLUDED.amount,
      city       = EXCLUDED.city,
      state      = EXCLUDED.state;
    """))
