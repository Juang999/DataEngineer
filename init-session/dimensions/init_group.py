from pathlib import Path
from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.dim_groups import transform_data_groups
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_source_db_exapro = config.env_source_exapro
    env_destination_db_warehouse = config.env_destination_warehouse
    file_query = Path(__file__).parent.parent.joinpath("queries\init_dim_groups.sql")

    extracted_data = extract_data_postgresql(connection=env_source_db_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_groups(extracted_data)

    load_dataframe(connection=env_destination_db_warehouse, schema="public", tablename="dim_groups", mode="append", df=transformed_data)

    print(transformed_data)