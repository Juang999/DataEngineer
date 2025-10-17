from pathlib import Path
from environment import Environment
from src.mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.dim.dim_account_master import transform_data_account_master
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_source_exapro = config.env_source_exapro
    env_destintation = config.env_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries\dim\init_dim_account_master.sql")

    extracted_data = extract_data_postgresql(connection=env_source_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_account_master(extracted_data)

    load_dataframe(connection=env_destintation, schema="public", tablename="dim_account_master", mode="append", df=transformed_data)

    print(transformed_data)