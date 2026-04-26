from datetime import date
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


from pipelines import spark_client, SparkReader, SparkWriter
from pipelines.models import (
    ReaderConfig,
    EnumIngestionMode,
    EnumMergeStrategy,
    EnumReadMode,
    WriterConfig,
    IcebergTableConfig,
)
from .curated_table import CuratedCoinsTable
from ..raw.raw_table import RawCoinsTable


class CuratedCoins:
    def __init__(self, date: date = date.today()):
        self.date = date
        self.source = RawCoinsTable()
        self.table = CuratedCoinsTable()
        self.spark: SparkSession = spark_client.get_session()
        self.reader = SparkReader()
        self.writer = SparkWriter()

    def get_data(self) -> DataFrame:
        return self.reader.get_data(
            ReaderConfig(
                source_table=self.source.full_name(),
                target_table=self.table.full_name(),
                date_column="dataHoraCotacao",
                mode=EnumReadMode.INCREMENTAL,
            )
        )

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
            df.withColumn(
                "spread",
                F.round(
                    (F.col("sales_cotation") - F.col("purchase_cotation"))
                    / F.col("purchase_cotation"),
                    6,
                ),
            )
            .withColumn(
                "mid_price", (F.col("purchase_cotation") + F.col("sales_cotation")) / 2
            )
            .withColumn(
                "spread_bps",
                F.round(
                    (
                        (F.col("sales_cotation") - F.col("purchase_cotation"))
                        / F.col("purchase_cotation")
                    )
                    * 10000,
                    2,
                ),
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

    def save(self, df: DataFrame) -> None:
        self.writer.write_data(
            df,
            WriterConfig(
                iceberg_table_cfg=IcebergTableConfig(
                    table_name=self.table.full_name(),
                    schema=self.table.schema(),
                    partitions=["symbol", "date_time_cotation"],
                ),
                mode=EnumIngestionMode.UPSERT,
                strategy=EnumMergeStrategy.TYPE1,
                merge_columns=["symbol", "date_time_cotation"],
            ),
        )

    def run(self):
        df = self.rename_columns()
        df = self.metrics_columns(df)
        df = self.apply_schema(df)
        self.save(df)
