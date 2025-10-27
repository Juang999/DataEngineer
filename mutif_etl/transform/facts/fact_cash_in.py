import polars as pl

def transform_data_cash_in(df):
    result = clean_data(df)

    return result

def clean_data(df):
    result = df.with_columns([
        pl.when(pl.col("cashin_remarks").is_null()).then(pl.lit('-')).otherwise(pl.col("cashin_remarks")).alias("cashin_remarks")
    ])

    return result