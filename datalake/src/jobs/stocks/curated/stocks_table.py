from pyspark.sql import types as t


class CuratedStocksTable:
    catalog = "hadoop_catalog"
    database = "curated"
    table = "stocks"

    @classmethod
    def full_name(cls):
        return f"{cls.catalog}.{cls.database}.{cls.table}"

    @classmethod
    def schema(cls) -> t.StructType:
        return t.StructType(
            [
                t.StructField("symbol", t.StringType(), True),
                t.StructField("datetime", t.DateType(), True),
                t.StructField("open", t.DoubleType(), True),
                t.StructField("high", t.DoubleType(), True),
                t.StructField("low", t.DoubleType(), True),
                t.StructField("close", t.DoubleType(), True),
                t.StructField("volume", t.LongType(), True),
                t.StructField("price_variation", t.DoubleType(), True),
                t.StructField("pct_variation", t.DoubleType(), True),
                t.StructField("range", t.DoubleType(), True),
                t.StructField("prev_close", t.DoubleType(), True),
                t.StructField("daily_return", t.DoubleType(), True),
                t.StructField("dt_reference", t.DateType(), False),
            ]
        )
