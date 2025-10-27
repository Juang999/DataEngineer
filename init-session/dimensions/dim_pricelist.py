from pathlib import Path
from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.dim_pricelist import transform_data_pricelist
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    config_db_exapro = config.env_source_exapro
    config_db_warehouse = config.env_destination_warehouse
    file_query = Path(__file__).parent.parent.joinpath("queries\init_dim_pricelist.sql")

    extracted_data = extract_data_postgresql(connection=config_db_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_pricelist(extracted_data)

    load_dataframe(connection=config_db_warehouse, schema="public", tablename="dim_pricelist_name", mode="append", df=transformed_data)

    print(transformed_data)