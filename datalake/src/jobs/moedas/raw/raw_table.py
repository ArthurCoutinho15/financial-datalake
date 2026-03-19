from pyspark.sql import types as t


class RawCoinsTable:
    catalog = "hadoop_catalog"
    database = "raw"
    table = "coins"

    @classmethod
    def full_name(cls):
        return f"{cls.catalog}.{cls.database}.{cls.table}"

    @classmethod
    def schema(cls):
        return t.StructType(
            [
                t.StructField("symbol", t.StringType(), False),
                t.StructField("paridadeCompra", t.StringType(), True),
                t.StructField("paridadeVenda", t.StringType(), True),
                t.StructField("cotacaoCompra", t.StringType(), True),
                t.StructField("cotacaoVenda", t.StringType(), True),
                t.StructField("dataHoraCotacao", t.StringType(), True),
                t.StructField("tipoBoletim", t.StringType(), True),
                t.StructField("dt_reference", t.DateType(), True),
            ]
        )
