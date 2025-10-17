from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.facts.fact_sales_shipment import transform_data_shipment
from mutif_etl.load.load_dataframe import load_dataframe


if __name__ == "__main__":
    config = Environment()

    env_exapro = config.source_exapro
    env_data_warehouse = config.env_prod_destination_warehouse
    file_marker_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/marker/facts/mark_sales_shipment.sql")
    file_fact_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/facts/init_fact_sales_shipment.sql")

    marked_data = extract_data_postgresql(connection=env_data_warehouse, filename=file_marker_query, conditions={})["created_at"][0]

    conditions = {
        "created_at": str(marked_data)
    }

    extracted_data = extract_data_postgresql(connection=env_exapro, filename=file_fact_query, conditions=conditions)
    transformed_data = transform_data_shipment(extracted_data)

    load_dataframe(connection=env_data_warehouse, schema="facts", tablename="fact_sales_shipment", mode="append", df=transformed_data)

    print('success to input new data...')
