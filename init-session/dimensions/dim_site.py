from pathlib import Path
from environment import Environment
from mutif_etl.load.load_dataframe import load_dataframe
from mutif_etl.extract.extract_postgresql import extract_data_postgresql

if __name__ == "__main__":
    config = Environment()
    env_source_exapro = config.env_source_exapro
    env_destionation_data_warehouse = config.env_destination_warehouse
    file_query = Path(__file__).parent.parent.joinpath("queries\init_dim_site.sql")

    extracted_data_site = extract_data_postgresql(connection=env_source_exapro, filename=file_query, conditions={})

    load_dataframe(connection=env_destionation_data_warehouse, schema="public", tablename="dim_sites", mode="append", df=extracted_data_site)

    print(extracted_data_site)