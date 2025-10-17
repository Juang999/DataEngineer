import polars as pl

def transform_data_curerncy(df):
    if len(df) > 0:
        result = cleansing_data(df)

        return result
    else:
        print("data doesn't exist")
        exit()

def cleansing_data(df):
    result = df.with_columns([
        pl.col('currency_name').str.to_uppercase()
    ])

    return result