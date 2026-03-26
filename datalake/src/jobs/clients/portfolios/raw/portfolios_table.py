import pyspark.sql.types as t


class RawPortfoliosTable:
    catalog = "hadoop_catalog"
    database = "raw"
    table = "portfolios"
    
    @classmethod
    def full_name(cls) -> str:
        return f"{cls.catalog}.{cls.database}.{cls.table}"
    
    @classmethod
    def schema(cls) -> t.StringType:
        return t.StructType([
            t.StructField("id", t.StringType(), False),
            t.StructField("client_id", t.StringType(), False),
            t.StructField("name", t.StringType(), False),
            t.StructField("created_at", t.StringType(), False),
            t.StructField("updated_at", t.StringType(), False),
            t.StructField("dt_reference", t.StringType(), False),
        ])