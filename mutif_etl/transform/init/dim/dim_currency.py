import polars as pl

def transform_data_curerncy(df):
    result = cleansing_data(df)

    return result

def cleansing_data(df):
    result = df.with_columns([
        pl.col('currency_name').str.to_titlecase()
    ])

    return result