from pathlib import Path
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.extract.extract_spreadsheet import extract_data_spreadsheet
from mutif_etl.transform.dim.dim_products import transform_data_products
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    config_db_exapro = config.source_exapro
    config_spreadsheet = config.env_spreadsheet
    config_db_warehouse = config.env_prod_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries\init\dim\init_dim_products.sql")

    extracted_data_postgresql = extract_data_postgresql(connection=config_db_exapro, filename=file_query, conditions={})
    extracted_data_spreadsheet = extract_data_spreadsheet(scopes=config_spreadsheet["scope"], sheet_id=config_spreadsheet["sheet_id"], worksheet="Dim Produk", credentials=config_spreadsheet["credentials"])

    transformed_data = transform_data_products(dataframe_spreadsheet=extracted_data_spreadsheet, dataframe_postgresql=extracted_data_postgresql)

    load_dataframe(connection=config_db_warehouse, schema="public", tablename="dim_products", mode="append", df=transformed_data)

    print(transformed_data)