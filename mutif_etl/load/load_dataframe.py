import logging
from typing import Dict, Any
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def load_dataframe(connection: Dict[str, Any], schema: str, tablename: str, mode: str, df):
    try:
        dialect = connection["dialect"]
        username = connection["username"]
        password = connection["password"]
        host = connection["host"]
        port = connection["port"]
        database = connection["database"]

        conn_str = f"{dialect}+psycopg2://{username}:{password}@{host}:{port}/{database}"

        logger.info("Start to load dataframe into table destination...")

        if len(df) == 0:
            logger.info("Data doesn't exist. Return empty dataframe")
            return

        engine = create_engine(conn_str)
        pandas_df = df.to_pandas()

        # gunakan raw_connection agar punya .cursor()
        # pandas_df.to_sql(
        #     name=tablename,
        #     con=engine,
        #     schema=schema,
        #     if_exists=mode,
        #     index=False
        # )
        with engine.connect() as conn:
            pandas_df.to_sql(
                name=tablename,
                con=conn,
                schema=schema,
                if_exists=mode,
                index=False,
                method="multi"  # lebih cepat untuk batch insert
            )

        logger.info(f"✅ Success to insert {len(pandas_df)} rows data into table {schema}.{tablename}")

    except Exception as e:
        logger.error(f"❌ Error loading data into table {tablename}: {e}", exc_info=True)
        raise RuntimeError("Failed to load data into table...")
