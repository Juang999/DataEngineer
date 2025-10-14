import os
from pathlib import Path
from dotenv import load_dotenv

class Environment():
    path_environment = Path(__file__).parent.joinpath(".env")
    load_dotenv(dotenv_path=path_environment)

    def __init__(self):
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        sheet_id = "1a2IrbnJqoqAhBOx3OXUwiq0-aoissKaUXXI_GqbtAd8"
        worksheet = "Dim Produk"
        path_credentials = str(Path(__file__).parent.joinpath("credentials.json"))

        self.source_exapro = {
            "dialect": os.getenv("DB_SOURCE_EXAPRO_DIALECT"),
            "username": os.getenv("DB_SOURCE_EXAPRO_USERNAME"),
            "password": os.getenv("DB_SOURCE_EXAPRO_PASSWORD"),
            "host": os.getenv("DB_SOURCE_EXAPRO_HOST"),
            "port": os.getenv("DB_SOURCE_EXAPRO_PORT"),
            "database": os.getenv("DB_SOURCE_EXAPRO_DATABASE"),
        }

        self.env_dev_destination_warehouse = {
            "dialect": os.getenv("DB_DEV_WAREHOUSE_DIALECT"),
            "username": os.getenv("DB_DEV_WAREHOUSE_USERNAME"),
            "password": os.getenv("DB_DEV_WAREHOUSE_PASSWORD"),
            "host": os.getenv("DB_DEV_WAREHOUSE_HOST"),
            "port": os.getenv("DB_DEV_WAREHOUSE_PORT"),
            "database": os.getenv("DB_DEV_WAREHOUSE_DATABASE"),
        }

        self.env_prod_destination_warehouse = {
            "dialect": os.getenv("DB_PROD_WAREHOUSE_DIALECT"),
            "username": os.getenv("DB_PROD_WAREHOUSE_USERNAME"),
            "password": os.getenv("DB_PROD_WAREHOUSE_PASSWORD"),
            "host": os.getenv("DB_PROD_WAREHOUSE_HOST"),
            "port": os.getenv("DB_PROD_WAREHOUSE_PORT"),
            "database": os.getenv("DB_PROD_WAREHOUSE_DATABASE"),
        }

        self.env_spreadsheet = {
            "scope": scopes,
            "sheet_id": sheet_id,
            "worksheet": worksheet,
            "credentials": path_credentials
        }