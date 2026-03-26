import pyspark.sql.types as t


class RawTransactionsTable:
    catalog = "hadoop_catalog"
    database = "raw"
    table = "transactions"

    @classmethod
    def full_name(cls) -> str:
        return f"{cls.catalog}.{cls.database}.{cls.table}"

    @classmethod
    def schema(cls) -> t.StringType:
        return t.StructType(
            [
                t.StructField("id", t.StringType(), False),
                t.StructField("position_id", t.StringType(), False),
                t.StructField("type", t.StringType(), False),
                t.StructField("quantity", t.StringType(), False),
                t.StructField("price_brl", t.StringType(), False),
                t.StructField("executed_at", t.StringType(), False),
                t.StructField("created_at", t.StringType(), False),
                t.StructField("dt_reference", t.StringType(), False),
            ]
        )
