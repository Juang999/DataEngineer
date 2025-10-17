from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.extract.extract_spreadsheet import extract_data_spreadsheet
from mutif_etl.transform.dim.dim_products import transform_data_products
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == '__main__':
    config = Environment()

    env_exapro = config.source_exapro
    env_data_warehouse = config.env_prod_destination_warehouse
    env_spreadsheet = config.env_spreadsheet
    file_marker_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/marker/dim/dim_products.sql")
    file_dim_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/dim/dim_products.sql")

    marked_data = extract_data_postgresql(connection=env_data_warehouse, filename=file_marker_query, conditions={})['created_at'][0]

    conditions = {
        "created_at": str(marked_data)
    }

    extracted_data_exapro = extract_data_postgresql(connection=env_exapro, filename=file_dim_query, conditions=conditions)
    extracted_data_spreadsheet = extract_data_spreadsheet(scopes=env_spreadsheet["scope"], sheet_id=env_spreadsheet["sheet_id"], worksheet="Dim Produk", credentials=env_spreadsheet["credentials"])

    transformed_data = transform_data_products(dataframe_postgresql=extracted_data_exapro, dataframe_spreadsheet=extracted_data_spreadsheet)

    load_dataframe(connection=env_data_warehouse, schema="public", tablename="dim_products", mode="append", df=transformed_data)

    print(transformed_data)

