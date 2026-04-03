import pyspark.sql.types as t 


class CuratedClientsTable:
    catalog = "hadoop_catalog"
    database = "curated"
    table = "clients"
    
    @classmethod
    def full_name(cls):
        return f"{cls.catalog}.{cls.database}.{cls.table}"

    @classmethod
    def schema(cls) -> t.StructType:
        return t.StructType(
            [
                t.StructField("client_id", t.StringType(), True),
                t.StructField("name", t.StringType(), True),
                t.StructField("email", t.StringType(), True),
                t.StructField("cpf", t.StringType(), True),
                t.StructField("phone", t.StringType(), True),
                t.StructField("city", t.StringType(), True),
                t.StructField("state", t.StringType(), True),
                t.StructField("portfolio_id", t.StringType(), True),
                t.StructField("portfolio_name", t.StringType(), True),
                t.StructField("position_id", t.StringType(), True),
                t.StructField("ticker", t.StringType(), True),
                t.StructField("asset_type", t.StringType(), True),
                t.StructField("position_quantity", t.DoubleType(), True),
                t.StructField("avg_price_brl", t.DoubleType(), True),
                t.StructField("transaction_id", t.StringType(), True),
                t.StructField("transaction_type", t.StringType(), True),
                t.StructField("transaction_quantity", t.DoubleType(), True),
                t.StructField("price_brl", t.DoubleType(), True),
                t.StructField("dt_reference", t.DateType(), True),
            ]
        )