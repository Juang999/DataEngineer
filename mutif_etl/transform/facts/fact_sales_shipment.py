import polars as pl

def transform_data_shipment(df):
    if len(df) == 0:
        print("Data doesn't exist")
        exit()
    else:
        result = sort_data_shipment(df)

    return result

def sort_data_shipment(df):
    result = df.sort("created_at")

    return result