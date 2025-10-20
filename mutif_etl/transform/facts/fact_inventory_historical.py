def transform_data_inventory_historical(df):
    if len(df) > 0 :
        result = sort_data(df)

        return result
    else:
        print("dataframe is empty")
        exit()

def sort_data(df):
    result = df.sort("created_at")

    return result