import polars as pl

def transform_data_copy_carbon(df):
    result = clean_data(df)

    return result

def clean_data(df):
    result = df.pipe(lambda df:
        df.drop_nulls(subset=["copy_carbon_id"])
    ).with_columns([
        pl.col("carbon_copy_desc").str.to_uppercase()
    ])

    return result