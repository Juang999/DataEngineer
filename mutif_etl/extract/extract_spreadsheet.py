import gspread
import logging
import polars as pl
from google.oauth2.service_account import Credentials

def extract_data_spreadsheet(scopes: list, credentials: str, sheet_id: str, worksheet: str, retries: int = 3):
    """
    function yang digunakan untuk mengambil data spreadsheet

    args:
        :param scopes: adalah list link google spreadsheet
        :param credentials: adalah credentials google spreadsheet
        :param sheet_id: spreadsheet id
        :param worksheet: worksheet name
        :param retries: pengulangan jika gagal mengambil data dari postgresql

    return:
        :polars.dataFrame: DataFrame yang berisi data dari spreadsheet
    """

    logger = logging.getLogger(__name__)

    for attempt in range(retries):
        try:
            logger.info(f"Percobaan ke-{attempt + 1}: Menghubungi Google Sheets API...")
            creds = Credentials.from_service_account_file(credentials, scopes=scopes)
            client = gspread.authorize(creds)

            sheet = client.open_by_key(sheet_id)

            dim_products = sheet.worksheet(worksheet)

            values_list = dim_products.get_all_values()

            if not values_list:
                logger.info(f"Worksheet kosong. Mengambilkan DataFrame kosong.")
                return pl.DataFrame()

            header_data = values_list[0]
            all_data = values_list[1:]
            df = pl.DataFrame(all_data, schema=header_data)

            logger.info(f"Berhasil mengambil {len(df)} baris data dari {worksheet}")
            return df
        except gspread.exceptions.APIError as e:
            logger.error(f"Error API Gspread: {e}")
            if "NOT_FOUND" in str(e) or attempt == retries - 1:
                raise
            logger.warning(f"Gagal, mencoba kembali...")
        except Exception as e:
            logger.error(f"Error: {e}", exc_info=True)
            if attempt == retries - 1:
                raise

    raise RuntimeError(f"Gagal mengambil data setelah beberapa kali percobaan")