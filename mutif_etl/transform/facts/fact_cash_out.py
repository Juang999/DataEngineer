import polars as pl

def transfrom_data_fact_cash_out(df):
    result = clean_data(df)

    return result

def clean_data(df):
    result = df.with_columns([
        pl.when(pl.col("cashout_amount_remains").is_null()).then(pl.lit(0)).otherwise(pl.col("cashout_amount_remains")).alias("cashout_amount_remains"),
        pl.when(pl.col("cashout_amount_realization").is_null()).then(pl.lit(0)).otherwise(pl.col("cashout_amount_realization")).alias("cashout_amount_realization"),
        pl.when(pl.col("cashout_request_code").is_null()).then(pl.lit('-')).otherwise(pl.col("cashout_request_code")).alias("cashout_request_code"),
        pl.when(pl.col("cashout_is_memo").is_null()).then(pl.lit('-')).otherwise(pl.col("cashout_is_memo")).alias("cashout_is_memo"),
        pl.when(pl.col("cashout_reference_code").is_null()).then(pl.lit('-')).otherwise(pl.col("cashout_reference_code")).alias("cashout_reference_code"),
        pl.when(pl.col("cashout_detail_remarks").is_null()).then(pl.lit('-')).otherwise(pl.col("cashout_detail_remarks")).alias("cashout_detail_remarks")
    ]).sort('created_at')

    return result