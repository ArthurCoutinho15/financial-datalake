from datetime import date
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from .curated_table import CuratedCoinsTable
from ..raw.raw_table import RawCoinsTable
from clients.spark_client import spark_client


class CuratedCoins:
    def __init__(self, date: date = date.today()):
        self.date = date
        self.source = RawCoinsTable()
        self.table = CuratedCoinsTable()
        self.spark: SparkSession = spark_client.get_session()

    def get_data(self) -> DataFrame:
        return self.spark.read.table(self.source.full_name())

    def rename_columns(self) -> DataFrame:
        coins = self.get_data()

        coins = coins.select(
            F.col("symbol").alias("symbol"),
            F.col("paridadeCompra").alias("purchase_parity"),
            F.col("paridadeVenda").alias("sales_parity"),
            F.col("cotacaoCompra").alias("purchase_cotation"),
            F.col("cotacaoVenda").alias("sales_cotation"),
            F.col("dataHoraCotacao").alias("date_time_cotation"),
            F.col("tipoBoletim").alias("bill_type"),
            F.col("dt_reference"),
        )

        return coins

    def metrics_columns(self, df: DataFrame) -> DataFrame:
        return (
                df
                .withColumn(
                    "spread",
                    F.round(
                        (F.col("sales_cotation") - F.col("purchase_cotation"))
                        / F.col("purchase_cotation"),
                        6,
                    ),
                )
                .withColumn(
                    "mid_price",
                    (F.col("purchase_cotation") + F.col("sales_cotation")) / 2
                )
                .withColumn(
                    "spread_bps",
                    F.round(((F.col("sales_cotation") - F.col("purchase_cotation")) / F.col("purchase_cotation")) * 10000, 2)
                )
            )

    def apply_schema(self, df: DataFrame) -> DataFrame:
        schema = self.table.schema()

        return df.select(
            *[
                F.col(field.name).cast(field.dataType).alias(field.name)
                for field in schema
            ]
        )

    def create_table(self):

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.table.full_name()} (
                symbol STRING,
                purchase_parity DOUBLE,
                sales_parity DOUBLE,
                purchase_cotation DOUBLE,
                sales_cotation DOUBLE,
                date_time_cotation TIMESTAMP,
                bill_type STRING,
                spread DOUBLE,
                mid_price DOUBLE,
                spread_bps DOUBLE,
                dt_reference DATE
            )
            USING iceberg
            PARTITIONED BY (dt_reference)
        """)

    def save(self, df: DataFrame) -> None:
        df.writeTo(self.table.full_name()).overwritePartitions()

    def run(self):
        df = self.rename_columns()
        df = self.metrics_columns(df)
        df = self.apply_schema(df)
        self.create_table()
        self.save(df)

        print(df.show())
        print(df.printSchema())
