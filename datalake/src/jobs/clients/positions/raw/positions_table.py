import pyspark.sql.types as t


class RawPositionsTable:
    catalog = "hadoop_catalog"
    database = "raw"
    table = "positions"
    
    @classmethod
    def full_name(cls) -> str:
        return f"{cls.catalog}.{cls.database}.{cls.table}"
    
    @classmethod
    def schema(cls) -> t.StringType:
        return t.StructType([
            t.StructField("id", t.StringType(), False),
            t.StructField("portfolio_id", t.StringType(), False),
            t.StructField("ticker", t.StringType(), False),
            t.StructField("asset_type", t.StringType(), False),
            t.StructField("quantity", t.StringType(), False),
            t.StructField("avg_price_brl", t.StringType(), False),
            t.StructField("created_at", t.StringType(), False),
            t.StructField("updated_at", t.StringType(), False),
            t.StructField("dt_reference", t.StringType(), False),
        ])