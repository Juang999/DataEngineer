import polars as pl

def transform_data_fund(df):
    result = clean_data(df)

    return result

def clean_data(df):
    result = df.pipe(lambda df:
        df.drop_nulls(subset=["fund_id"])
    ).with_columns([
        pl.when(pl.col("fund_code").is_null()).then(pl.lit("-")).when(pl.col("fund_code") == pl.lit("")).then(pl.lit("-")).otherwise(pl.col("fund_code")).alias("fund_code"),
        pl.when(pl.col("fund_name").is_null()).then(pl.lit("-")).when(pl.col("fund_name") == pl.lit("")).then(pl.lit("-")).otherwise(pl.col("fund_name").str.to_titlecase()).alias("fund_name")
    ])

    return result