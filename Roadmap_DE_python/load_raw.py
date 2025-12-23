import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

engine = create_engine(
    "postgresql+psycopg://postgres:1234@localhost:5432/postgres"
)

df = pd.read_csv("/home/wipawee/Roadmap_DE_python/raw/raw_order_items_dataset.csv")

df.to_sql(
    
    name="raw_order_items_dataset",
    con=engine,
    schema="public",
    if_exists="replace",
    index=False
)


