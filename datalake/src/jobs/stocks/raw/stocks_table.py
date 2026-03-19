from pyspark.sql import types as t


class RawStocksTable:
    catalog = "hadoop_catalog"
    database = "raw"
    table = "stocks"

    @classmethod
    def full_name(cls):
        return f"{cls.catalog}.{cls.database}.{cls.table}"

    @classmethod
    def schema(cls) -> t.StructType:
        return t.StructType(
            [
                t.StructField("symbol", t.StringType(), True),
                t.StructField("datetime", t.StringType(), True),
                t.StructField("open", t.StringType(), True),
                t.StructField("high", t.StringType(), True),
                t.StructField("low", t.StringType(), True),
                t.StructField("close", t.StringType(), True),
                t.StructField("volume", t.StringType(), True),
                t.StructField("dt_reference", t.DateType(), True),
            ]
        )
