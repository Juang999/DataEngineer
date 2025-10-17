import logging

logger = logging.getLogger(__name__)

def transform_data_entities(df):
    try:
        if len(df) > 0:
            logger.info("Start transform data entities...")
            result = rename_columns(df)

            return result
        else:
            print("data doesn't exist")
            exit()
    except Exception as e:
        logger.error(f"Error: {e}")
        raise RuntimeError(f"Error while transforming data entities")

def rename_columns(df):
    result = df.rename({
        "en_id": "entity_id",
        "en_desc": "entity_desc",
        "en_add_date": "created_at"
    })

    return result