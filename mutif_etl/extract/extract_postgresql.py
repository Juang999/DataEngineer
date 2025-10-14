import logging
import polars as pl
import connectorx as cx
from typing import Dict, Any

def read_query_sql(query_file_path: str) -> str:
    with open(query_file_path, "r") as query_file:
        return query_file.read()

def extract_data_postgresql(connection: Dict[str, Any], filename: str, conditions: Dict[str, Any], retries: int = 3):
    logger = logging.getLogger(__name__)

    dialect = connection['dialect']
    username = connection['username']
    password = connection['password']
    host = connection['host']
    port = connection['port']
    database = connection['database']

    conn = f"{dialect}://{username}:{password}@{host}:{port}/{database}"

    path_file_query = str(filename)

    query = read_query_sql(path_file_query)

    if len(conditions.items()) > 0:
        for key, value in conditions.items():
            query = query.replace("{"+key+"}", value)

    for attempt in range(retries):
        try:
            logger.info(f"({attempt + 1}). Start to get data from PostgreSQL...")

            result = cx.read_sql(conn, query, return_type="polars")

            if result is None:
                logger.info(f"Data doesn't exist. Return empty DataFrame")
                return pl.DataFrame()

            logger.info(f"Success to get {len(result)} rows data from f{filename}")
            return result
        except Exception as e:
            logger.warning(f"Error: {e}")
            if attempt == retries - 1:
                raise

    raise RuntimeError(f"Gagal mengambil data setelah beberapa kali percobaan")