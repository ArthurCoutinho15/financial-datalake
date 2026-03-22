import pyspark.sql.types as t 


class CuratedCryptoTable:
    catalog = "hadoop_catalog"
    database = "curated"
    table = "crypto"
    
    @classmethod
    def full_name(cls):
        return f"{cls.catalog}.{cls.database}.{cls.table}"
    
    @classmethod
    def schema(cls) -> t.StructType:
        return t.StructType([
            t.StructField("symbol", t.StringType(), True),
            t.StructField("datetime", t.DateType(), True),
            t.StructField("open", t.DoubleType(), True),
            t.StructField("high", t.DoubleType(), True),
            t.StructField("low", t.DoubleType(), True),
            t.StructField("close", t.DoubleType(), True),
            t.StructField("return_1d", t.DoubleType(), True),
            t.StructField("pct_change", t.DoubleType(), True),
            t.StructField("range", t.DoubleType(), True),
            t.StructField("dt_reference", t.DateType(), True),
        ])