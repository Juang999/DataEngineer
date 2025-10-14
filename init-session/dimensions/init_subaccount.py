from pathlib import Path
from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_exapro = config.dev_source_exapro
    env_destination = config.env_prod_destination_warehouse
    query_file = Path(__file__).parent.parent.parent.joinpath(r"queries\dim\init_dim_subaccount.sql")

    extracted_data = extract_data_postgresql(connection=env_exapro, filename=query_file, conditions={})

    load_dataframe(connection=env_destination, schema="public", tablename="dim_subaccount", mode="append", df=extracted_data)

    print(extracted_data)