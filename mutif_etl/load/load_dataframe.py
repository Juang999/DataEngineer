import logging
import polars as pl
from typing import Dict, Any
from sqlalchemy import create_engine, insert, MetaData, Table, Column, Integer, String

logger = logging.getLogger(__name__)

def load_dataframe(connection: Dict[str, Any], tablename, schema, mode, df):
    try:
        dialect = connection["dialect"]
        username = connection["username"]
        password = connection["password"]
        host = connection["host"]
        port = connection["port"]
        database = connection["database"]

        conn = f"{dialect}+psycopg2://{username}:{password}@{host}:{port}/{database}"

        logger.info("Start to load dataframe into table destination...")

        if len(df) == 0:
            logger.info("Data doesn't exist. Return empty dataframe")
            return

        engine = create_engine(conn, echo=True)
        pandas_df = df.to_pandas()

        pandas_df.to_sql(
            tablename,
            con=engine,
            schema=schema,
            if_exists=mode,
            index=False
        )

        logger.info(f"Succeess to insert {len(pandas_df)} rows data into table {tablename}")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise RuntimeError("Failed to load data into table...")
