import pyspark.sql.types as t


class CuratedCoinsTable:
    catalog = "hadoop_catalog"
    database = "curated"
    table = "curated_coins"

    @classmethod
    def full_name(cls):
        return f"{cls.catalog}.{cls.database}.{cls.table}"

    @classmethod
    def schema(cls) -> t.StructType:
        return t.StructType(
            [
                t.StructField("symbol", t.StringType(), False),
                t.StructField("purchase_parity", t.DoubleType(), True),
                t.StructField("sales_parity", t.DoubleType(), True),
                t.StructField("purchase_cotation", t.DoubleType(), True),
                t.StructField("sales_cotation", t.DoubleType(), True),
                t.StructField("date_time_cotation", t.TimestampType(), True),
                t.StructField("bill_type", t.StringType(), True),
                t.StructField("spread", t.DoubleType(), True),
                t.StructField("dt_reference", t.DateType(), True),
            ]
        )
