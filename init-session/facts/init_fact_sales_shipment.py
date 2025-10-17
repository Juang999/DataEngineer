from pathlib import Path
from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.facts.fact_sales_shipment import transform_data_shipment
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == "__main__":
    config = Environment()

    env_src_db_exapro = config.source_exapro
    env_dst_db_warehouse = config.env_dev_destination_warehouse
    file_query = Path(__file__).parent.parent.parent.joinpath(r"queries\init\facts\init_fact_sales_shipment.sql")

    extracted_data = extract_data_postgresql(connection=env_src_db_exapro, filename=file_query, conditions={})
    transformed_data = transform_data_shipment(extracted_data)

    load_dataframe(connection=env_dst_db_warehouse, schema="facts", tablename="fact_sales_shipment", mode="append", df=transformed_data)

    print(transformed_data)