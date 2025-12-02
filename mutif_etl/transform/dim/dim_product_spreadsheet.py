import logging
import polars as pl

logger = logging.getLogger(__name__)

"""
    Returns:
        Main function dim_products transform
"""
def transform_data_product_spreadsheet(dataframe_spreadsheet, dataframe_postgresql):
    if len(dataframe_spreadsheet) > 0:
        logger.info("Start transforming data...")
        selected_data_postgresql = select_data_postgresql(dataframe_postgresql)
        selected_data_spreadsheet = select_data_spreadsheet(dataframe_spreadsheet)

        joined_df = join_dataframes(dataframe_spreadsheet=selected_data_spreadsheet, dataframe_postgresql=selected_data_postgresql)

        cleaned_data = clean_data(joined_df)
        
        selected_columns = select_spesific_columns(cleaned_data)

        return selected_columns
    else:
        print("data doesn't exist")
        exit()

def clean_data(df):
    result = df.pipe(lambda df:
            df.drop_nulls(subset=['product_id'])
                .unique(subset=['product_id'], keep="first")
        )

    return result

def select_data_postgresql(df):
    result = df.rename({
        "pt_id": "product_id",
        "pt_en_id": "entity_id",
        "pt_code": "partnumber"
    }).select([
        "product_id",
        "entity_id",
        "partnumber"
    ])

    return result

def select_data_spreadsheet(df):
    logger.info("Rename Spreadhseet DataFrame columns")

    result = df.with_columns(
        pl.when(pl.col("pt_code").is_not_null()).then(pl.col("pt_code")).otherwise(pl.lit("-")).alias("partnumber"),
        pl.when(pl.col("pt_desc").is_not_null()).then(pl.col("pt_desc")).otherwise(pl.lit("-")).alias("description1"),
        pl.when(pl.col("pt_desc2").is_not_null()).then(pl.col("pt_desc2")).otherwise(pl.lit("-")).alias("description2"),
        pl.when(pl.col("product_group").is_not_null()).then(pl.col("product_group")).otherwise(pl.lit("-")).alias("product_group"),
        pl.when(pl.col("product_grade").is_not_null()).then(pl.col("product_grade")).otherwise(pl.lit("-")).alias("product_grade"),
        pl.when((pl.col("cost").is_null()) | (pl.col("cost") == "")).then(pl.lit(None)).otherwise(pl.col("cost").str.replace(r"\.", "")).alias("cost"),
        pl.when((pl.col("price").is_null()) | (pl.col("price") == "")).then(pl.lit(None)).otherwise(pl.col("price").str.replace(r"\.", "")).alias("price"),
        pl.when(pl.col("classification").is_not_null()).then(pl.col("classification")).otherwise(pl.lit("-")).alias("classification"),
        pl.when(pl.col("category").is_not_null()).then(pl.col("category")).otherwise(pl.lit("-")).alias("category"),
        pl.when(pl.col("sub_category").is_not_null()).then(pl.col("sub_category")).otherwise(pl.lit("-")).alias("subcategory"),
        pl.when(pl.col("type").is_not_null()).then(pl.col("type")).otherwise(pl.lit("-")).alias("type"),
        pl.when(pl.col("size").is_not_null()).then(pl.col("size")).otherwise(pl.lit("-")).alias("size"),
        pl.when(pl.col("release_year").is_not_null()).then(pl.col("release_year")).otherwise(pl.lit("-")).alias("release_year"),
        pl.when((pl.col('date_create').is_not_null()) | (pl.col("date_create") != "")).then(pl.col("date_create").str.strptime(pl.Date, format="%d/%m/%Y", strict=False).dt.strftime("%Y%m%d")).otherwise(pl.lit(None)).alias("date_id"),
        pl.when(pl.col("pl_desc").is_not_null()).then(pl.col("pl_desc")).otherwise(pl.lit("-")).alias("productline_desc"),
        pl.col("created_at").alias("created_at")
    ).select([
        "partnumber",
        "description1",
        "description2",
        "product_group",
        "product_grade",
        "cost",
        "price",
        "classification",
        "category",
        "subcategory",
        "type",
        "size",
        "release_year",
        "date_id",
        "productline_desc",
        "created_at"
    ])

    return result

"""
    Returns:
        Joined renamed DataFrames
"""
def join_dataframes(dataframe_spreadsheet, dataframe_postgresql):
    logger.info("Join DataFrames")

    result = dataframe_spreadsheet.join(dataframe_postgresql, on="partnumber", how="left")

    return result

"""
    Returns:
        Spesific columns
"""
def select_spesific_columns(df):
    result = df.select([
        'product_id',
        'date_id',
        'entity_id',
        'partnumber',
        'description1',
        'description2',
        'product_group',
        'product_grade',
        'cost',
        'price',
        'classification',
        'category',
        'subcategory',
        'type',
        'size',
        'release_year',
        'productline_desc',
        'created_at'
    ]).filter(pl.col("product_id").is_not_null()).sort("product_id")

    return result

def columns_condition(df):
    result = df.with_columns([
        pl.when(pl.col.description2 is None).then(pl.lit("-")).when(pl.col.description2 == '').then(None).otherwise(pl.col.description2).alias('description2'),
        pl.when(pl.col.kelompok_produk is None).then(pl.lit("-")).when(pl.col.kelompok_produk == '').then(None).otherwise(pl.col.kelompok_produk).alias('kelompok_produk'),
        pl.when(pl.col.produk_grade is None).then(pl.lit("-")).when(pl.col.produk_grade == '').then(None).otherwise(pl.col.produk_grade).alias('produk_grade'),
        pl.when(pl.col.kelompok is None).then(pl.lit("-")).when(pl.col.kelompok == '').then(None).otherwise(pl.col.kelompok).alias('kelompok'),
        pl.when(pl.col.sub_kategori is None).then(pl.lit("-")).when(pl.col.sub_kategori == '').then(None).otherwise(pl.col.sub_kategori).alias('sub_kategori'),
        pl.when(pl.col.jenis is None).then(pl.lit("-")).when(pl.col.jenis == '').then(None).otherwise(pl.col.jenis).alias('jenis'),
        pl.when(pl.col.ukuran is None).then(pl.lit("-")).when(pl.col.ukuran == '').then(None).otherwise(pl.col.ukuran).alias('ukuran'),
        pl.when(pl.col.cost.is_in([None, ''])).then(pl.lit(0)).otherwise(pl.col.cost.str.replace_all('\.', '')).alias('cost'),
        pl.when(pl.col.price.is_in([None, ''])).then(pl.lit(0)).otherwise(pl.col.price.str.replace_all(r'\.', '')).alias('price'),
        pl.when(pl.col.harga_satset.is_in([None, ''])).then(pl.lit(0)).otherwise(pl.col.harga_satset.str.replace_all(r'\.', '')).alias('harga_satset'),
        pl.when(pl.col.tahun_launching.is_in([None, '', '-'])).then(None).otherwise(pl.col.tahun_launching).alias('tahun_launching'),
        pl.when(pl.col.season_lebaran.is_in([None, '', '-'])).then(None).otherwise(pl.col.season_lebaran).alias('season_lebaran'),
    ])

    return result

def filter_data(df, conditions):
    result = df.filter([
        pl.col.partnumber == conditions['partnumber']
    ])

    return result