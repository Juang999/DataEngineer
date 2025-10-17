import logging
import polars as pl

logger = logging.getLogger(__name__)

"""
    Returns:
        Main function dim_products transform
"""
def transform_data_products(dataframe_spreadsheet, dataframe_postgresql):
    try:
        if len(dataframe_postgresql) > 0 :
            logger.info("Start transforming data...")
            cleaned_data_postgresql = clean_data_postgresql(dataframe_postgresql)

            selected_data_postgresql = select_data_postgresql(cleaned_data_postgresql)
            selected_data_spreadsheet = select_data_spreadsheet(dataframe_spreadsheet)

            joined_df = join_dataframes(dataframe_spreadsheet=selected_data_spreadsheet, dataframe_postgresql=selected_data_postgresql)

            selected_columns = select_spesific_columns(joined_df)

            return selected_columns
        else :
            print("data product doesn't exist")
            exit()
    except Exception as e:
        logger.error(f"Error: {e}")

        raise RuntimeError("Terjadi error saat mentransform data...")

def clean_data_postgresql(df):
    result = df.pipe(lambda df:
            df.drop_nulls(subset=['pt_id'])
                .unique(subset=['pt_id'], keep="first")
        )

    return result

def select_data_postgresql(df):
    result = df.rename({
        "pt_id": "product_id",
        "pt_en_id": "entity_id",
        "pt_code": "partnumber",
        "pt_desc1": "description1",
        "pt_desc2": "description2",
        "launch_year": "tahun_launching",
        "pt_class": "produk_grade"
    }).select([
        "product_id",
        "entity_id",
        "partnumber",
        "description1",
        "description2",
        "created_at",
        "date_id",
        "product_line_name",
        "price",
        "cost",
        "tahun_launching",
        "produk_grade"
    ])

    return result

def select_data_spreadsheet(df):
    logger.info("Rename Spreadhseet DataFrame columns")


    result = df.with_columns(
        pl.when(pl.col("Kelompok Produk").is_not_null()).then(pl.col("Kelompok Produk")).otherwise(pl.lit("-")).alias("kelompok_produk"),
        pl.when(pl.col("Kelompok").is_not_null()).then(pl.col("Kelompok")).otherwise(pl.lit("-")).alias("kelompok"),
        pl.when(pl.col("Kategori").is_not_null()).then(pl.col("Kategori")).otherwise(pl.lit("-")).alias("kategori"),
        pl.when(pl.col("Sub Kategori").is_not_null()).then(pl.col("Sub Kategori")).otherwise(pl.lit("-")).alias("sub_kategori"),
        pl.when(pl.col("Jenis").is_not_null()).then(pl.col("Jenis")).otherwise(pl.lit("-")).alias("jenis"),
        pl.when(pl.col("Ukuran").is_not_null()).then(pl.col("Ukuran")).otherwise(pl.lit("-")).alias("ukuran"),
        pl.when(pl.col("Harga Set Set").is_null()).then(pl.lit(0)).otherwise(pl.col("Harga Set Set")).alias("harga_satset")
    ).rename({
        "Part Number": "partnumber",
        "Season Lebaran": "season_lebaran"
    }).select([
        "partnumber",
        "kelompok_produk",
        "kelompok",
        "kategori",
        "sub_kategori",
        "jenis",
        "ukuran",
        "season_lebaran",
        "harga_satset"
    ])

    return result

"""
    Returns:
        Joined renamed DataFrames
"""
def join_dataframes(dataframe_spreadsheet, dataframe_postgresql):
    logger.info("Join DataFrames")

    result = dataframe_postgresql.join(dataframe_spreadsheet, on="partnumber", how="left")

    return result

"""
    Returns:
        Spesific columns
"""
def select_spesific_columns(df):
    result = df.with_columns([
        pl.lit(None).alias("harga_satset"),
        pl.when(pl.col("kelompok_produk").is_null()).then(pl.lit(("-"))).otherwise(pl.col("kelompok_produk")).alias("kelompok_produk"),
        pl.when(pl.col("kelompok").is_null()).then(pl.lit(("-"))).otherwise(pl.col("kelompok")).alias("kelompok"),
        pl.when(pl.col("kategori").is_null()).then(pl.lit(("-"))).otherwise(pl.col("kategori")).alias("kategori"),
        pl.when(pl.col("sub_kategori").is_null()).then(pl.lit(("-"))).otherwise(pl.col("sub_kategori")).alias("sub_kategori"),
        pl.when(pl.col("jenis").is_null()).then(pl.lit(("-"))).otherwise(pl.col("jenis")).alias("jenis"),
        pl.when(pl.col("ukuran").is_null()).then(pl.lit(("-"))).otherwise(pl.col("ukuran")).alias("ukuran"),
        pl.when(pl.col("tahun_launching").is_null()).then(pl.lit("-")).otherwise(pl.col("tahun_launching")).alias("tahun_launching"),
        pl.when(pl.col("season_lebaran").is_null()).then(pl.lit("-")).when(pl.col('season_lebaran') == pl.lit('')).then(pl.lit("-")).otherwise(pl.col("season_lebaran")).alias("season_lebaran"),
        pl.when(pl.col("cost").is_null()).then(pl.lit(0)).otherwise(pl.col("cost")).alias("cost"),
    ]).select([
        'product_id',
        'date_id',
        'entity_id',
        'partnumber',
        'description1',
        'description2',
        'kelompok_produk',
        'produk_grade',
        'kelompok',
        'kategori',
        'sub_kategori',
        'jenis',
        'ukuran',
        'cost',
        'price',
        'harga_satset',
        'tahun_launching',
        'season_lebaran',
        'created_at',
        'product_line_name'
    ]).sort("product_id")

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