import os
import sys
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from environment import Environment
from mutif_etl.extract.extract_postgresql import extract_data_postgresql
from mutif_etl.transform.dim.dim_fund import transform_data_fund
from mutif_etl.load.load_dataframe import load_dataframe

if __name__ == '__main__':
    config = Environment()

    env_exapro = config.source_exapro
    env_data_warehouse = config.env_prod_destination_warehouse
    file_marker_query = Path(__file__).parent.parent.parent.joinpath(r"queries\inc\marker\dim\dim_entities.sql")
    file_dim_query = Path(__file__).parent.parent.parent.joinpath(r"queries\inc\dim\dim_entities.sql")
    
    marked_data = extract_data_postgresql(connection=env_data_warehouse, filename=file_marker_query, conditions={})['created_at'][0]

    conditions = {
        "created_at": str(marked_data)
    }

    extracted_data_exapro = extract_data_postgresql(connection=env_exapro, filename=file_dim_query, conditions=conditions)
    transformed_data = transform_data_fund(extracted_data_exapro)

    load_dataframe(connection=env_data_warehouse, schema="public", tablename="dim_fund", mode="append", df=extracted_data_exapro)

    print(transformed_data)