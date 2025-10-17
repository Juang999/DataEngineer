import logging
import polars as pl

logger = logging.getLogger(__name__)

def transform_data_groups(data):
    try:
        if len(data) > 0:
            logger.info("Start transforming data group...")

            cleaned_data = cleansing_data(data)
            result = sort_data(cleaned_data)

            logger.info("Transformed data group...")

            return result
        else:
            print("data doesn't exist")
            exit()

    except Exception as error:
        logger.error(error)

def rename_columns(df):
    result = df.rename({
        "group_code": "mitra_code"
    })

    return result

def cleansing_data(data):
    result = data.with_columns([
        pl.when(pl.col("group_code") == "DEPT-STORE").then(
            pl.col("group_code").replace("DEPT-STORE", "DST")).when(
            pl.col("group_code") == "online-store").then(
            pl.col("group_code").replace("online-store", "OL-STR")).otherwise(pl.col("group_code")),
        pl.when(pl.col("group_name") == "DEPT-STORE").then(
            pl.col("group_name").str.replace("-", " ").str.to_titlecase()).when(
            pl.col("group_name") == "online-store").then(
            pl.col("group_name").str.replace("-", " ").str.to_titlecase()).otherwise(pl.col("group_name")),
        pl.when(pl.col("group_description") == "DEPT-STORE").then(
            pl.col("group_description").str.replace("-", " ").str.to_titlecase()).when(
            pl.col("group_description") == "online-store").then(
            pl.col("group_description").str.replace("-", " ").str.to_titlecase()).otherwise(pl.col("group_description"))
    ])

    return result

def sort_data(df):
    result = df.sort("group_id")

    return result