from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.facts.fact_inventory_historical import transform_data_inventory_historical
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_exapro = config.source_exapro
    env_destination_data_warehouse = config.env_prod_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries\init\facts\fact_inventory_historical.sql")

    extracted_data = extract_data_postgresql(connection=env_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_inventory_historical(extracted_data)

    load_dataframe(connection=env_destination_data_warehouse, schema="facts", tablename="fact_inventory_historical", mode="append", df=transformed_data)

    print(env_destination_data_warehouse)
    print(transformed_data)