from pathlib import Path
import sys
import os
import polars as pl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.extract.extract_spreadsheet import extract_data_spreadsheet
from mutif_etl.transform.dim.dim_product_spreadsheet import transform_data_product_spreadsheet
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    config_db_exapro = config.source_exapro
    config_spreadsheet = config.env_spreadsheet
    config_db_warehouse = config.env_prod_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries/init/dim/init_dim_products.sql")
    file_marker_query = Path(__file__).parent.parent.parent.joinpath(r"queries/inc/marker/dim/dim_product_spreadsheet.sql")
    sheet_id = config_spreadsheet["spreadsheets_id"]["data_dimension"]
    worksheet = config_spreadsheet["worksheets"]["dimension_dim_product"]
    
    marked_data = extract_data_postgresql(connection=config_db_warehouse, filename=file_marker_query, conditions={})['created_at'][0]

    extracted_data_postgresql = extract_data_postgresql(connection=config_db_exapro, filename=file_query, conditions={})
    extracted_data_spreadsheet = extract_data_spreadsheet(scopes=config_spreadsheet["scope"], sheet_id=sheet_id, worksheet=worksheet, credentials=config_spreadsheet["credentials"])

    filtered_data_spreadsheet = extracted_data_spreadsheet.with_columns(pl.col('created_at').str.to_datetime(format="%Y-%m-%d %H:%M:%S")).filter(pl.col('created_at') > marked_data)

    transformed_data = transform_data_product_spreadsheet(filtered_data_spreadsheet, extracted_data_postgresql)
    
    load_dataframe(connection=config_db_warehouse, schema="public", tablename="dim_product_spreadsheet", mode="append", df=transformed_data)

    print(extract_data_spreadsheet)