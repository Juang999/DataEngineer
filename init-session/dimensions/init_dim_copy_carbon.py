from pathlib import Path
from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.init.dim.dim_copy_carbon import transform_data_copy_carbon
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_exapro = config.env_source_exapro
    env_destination = config.env_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries\dim\init_dim_copy_carbon.sql")

    extracted_data = extract_data_postgresql(connection=env_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_copy_carbon(extracted_data)

    load_dataframe(connection=env_destination, schema="public", tablename="dim_copy_carbon", mode="append", df=transformed_data)

    print(transformed_data)
