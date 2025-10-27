import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.facts.fact_cash_in import transform_data_cash_in
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_src_db_exapro = config.source_exapro
    env_dst_db_warehouse = config.env_prod_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries\init\facts\fact_cash_in.sql")

    extracted_data = extract_data_postgresql(connection=env_src_db_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_cash_in(extracted_data)

    try:
        load_dataframe(connection=env_dst_db_warehouse, schema="facts", tablename="fact_cash_in", mode="append", df=transformed_data)
    except Exception as error:
        print(error)
    
    print(transformed_data)