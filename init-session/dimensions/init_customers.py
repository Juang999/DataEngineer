from pathlib import Path
from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.extract.extract_spreadsheet import extract_data_spreadsheet
from mutif_etl.transform.dim_customers import transform_dim_customers
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()
    env_db_exapro = config.env_source_exapro
    env_spreadsheet = config.env_spreadsheet
    env_db_warehouse = config.env_destination_warehouse
    file_query = Path(__file__).parent.parent.joinpath("queries\init_dim_customers.sql")

    extracted_data_exapro = extract_data_postgresql(connection=env_db_exapro, filename=file_query, conditions={})
    extracted_data_spreadsheet = extract_data_spreadsheet(scopes=env_spreadsheet["scope"], sheet_id=env_spreadsheet["sheet_id"], worksheet=env_spreadsheet["worksheet"], credentials=env_spreadsheet["credentials"])

    transformed_data_customer = transform_dim_customers(df_postgresql=extracted_data_exapro, df_spreadsheet=extracted_data_spreadsheet)

    load_dataframe(connection=env_db_warehouse, schema="public", tablename="dim_customers", mode="append", df=transformed_data_customer)

