import polars as pl

def transform_data_pricelist(df):
    if len(df) > 0:
        result = cleansing_data(df)

        return result
    else:
        print("data doesn't exist")
        exit()

def cleansing_data(df):
    result = df.pipe(lambda df:
        df.drop_nulls(subset=["pricelist_id"])
    ).with_columns([
        pl.col("pricelist_name").str.to_titlecase()
    ])

    return result