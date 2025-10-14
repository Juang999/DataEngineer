import polars as pl

def transform_data_account_master(df):
    resize_name = resize_data_name(df)
    result = cleansing_data(resize_name)

    return result

def resize_data_name(df):
    result = df.with_columns([
        pl.col("account_name").str.to_titlecase()
    ])

    return result

def cleansing_data(df):
    result = df.pipe(
        lambda df:
            df.drop_nulls(subset=["account_id"])
    ).select([
        'account_id',
        'account_code',
        'account_name',
        'account_parent_id',
        'account_type',
        'sum_level',
        'account_sign',
        'is_active',
        'currency_id',
        'account_is_budget',
        'hierarchy_code',
        'created_at'
    ])

    return result
