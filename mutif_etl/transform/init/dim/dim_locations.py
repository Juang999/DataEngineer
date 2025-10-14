import polars as pl

def transform_data_location(df):
    result = cleansing_data_location(df)

    return result

def cleansing_data_location(df):
    result = df \
            .pipe(lambda df:
                    df.drop_nulls(subset=["location_id"])
            ) \
            .with_columns([
                pl.col("location_name").str.to_titlecase()
            ])

    return result