import polars as pl
import logging

logger = logging.getLogger(__name__)

def transform_dim_customers(df_postgresql, df_spreadsheet):
    try:
        logger.info("Start to transforming data customers...")

        spreadsheet_rename_column = rename_column_spreadsheet(df_spreadsheet)
        postgresql_rename_column = rename_column_postgresql(df_postgresql)

        cleansing_data_postgresql = cleansing_column_data(postgresql_rename_column)
        cleansing_data_spreadsheet = cleansing_column_data(spreadsheet_rename_column)

        join_data_source = join_data(df_postgresql=cleansing_data_postgresql, df_spreadsheet=cleansing_data_spreadsheet)

        result = select_column(join_data_source).sort("customer_id")

        logger.info("Finished transforming data customers.")

        return result
    except Exception as error:
        logger.error(error)

def rename_column_spreadsheet(df):
    result = df.rename({
        "Nama Mitra": "nama_mitra",
        "Customer Name": "customer_name",
        "Kabupaten/Kota": "kota_kabupaten",
        "Provinsi": "provinsi",
        "CS": "customer_service",
        "Area": "area"
    })

    return result

def rename_column_postgresql(df):
    result = df.rename({
        "customer_code": "mitra_code"
    })

    return result

def cleansing_column_data(df):
    result = df.with_columns([
        pl.col("nama_mitra").str.to_titlecase()
    ])

    return result

def join_data(df_postgresql, df_spreadsheet):
    result = df_postgresql.join(df_spreadsheet, on="nama_mitra", how="left")

    return result

def select_column(df):
    result = df.pipe(
        lambda df:
            df.drop_nulls(subset=["customer_id"])
                .unique(subset=["customer_id"], keep="first")
    ).select([
        'customer_id',
        'entity_id',
        'nama_mitra',
        'customer_name',
        'group_id',
        'kota_kabupaten',
        'provinsi',
        'customer_service',
        'area',
        'date_id',
        'created_at',
        'mitra_code'
    ])

    return result