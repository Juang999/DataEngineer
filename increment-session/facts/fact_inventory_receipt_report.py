from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.facts.fact_inventory_receipt_report import transform_data_inventory_receipt_report
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_exapro = config.source_exapro
    env_destination_data_warehouse = config.env_prod_destination_warehouse
    file_marker_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/marker/facts/mark_inventory_receipt_report.sql")
    file_fact_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/facts/fact_inventory_receipt_report.sql")

    marked_data = extract_data_postgresql(connection=env_destination_data_warehouse, filename=file_marker_query, conditions={})["created_at"][0]

    conditions = {
        "created_at": str(marked_data)
    }

    extracted_data = extract_data_postgresql(connection=env_exapro, filename=file_fact_query, conditions=conditions)
    transformed_data = transform_data_inventory_receipt_report(extracted_data)

    load_dataframe(connection=env_destination_data_warehouse, schema="facts", tablename="fact_inventory_receipt_report", mode="append", df=transformed_data)

    print(transformed_data)