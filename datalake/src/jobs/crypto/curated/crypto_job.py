from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window

from datalake.src.pipelines.models.writer import EnumIngestionMode, EnumMergeStrategy

from .crypto_table import CuratedCryptoTable
from ..raw.crypto_table import RawCryptoTable

from pipelines import spark_client, SparkReader, SparkWriter
from pipelines.models import (
    IcebergTableConfig,
    ReaderConfig,
    EnumReadMode,
    WriterConfig,
)


class CuratedCryptoJob:
    def __init__(self, date: date = date.today()):
        self.date = date
        self.source = RawCryptoTable()
        self.table = CuratedCryptoTable()
        self.spark: SparkSession = spark_client.get_session()
        self.reader = SparkReader()
        self.writer = SparkWriter()

    def get_data(self) -> DataFrame:
        return self.reader.get_data(
            ReaderConfig(
                source_table=self.source.full_name(),
                target_table=self.table.full_name(),
                date_column="datetime",
                mode=EnumReadMode.INCREMENTAL_WITH_HISTORY,
            )
        )

    def cast_columns(self, df: DataFrame) -> DataFrame:
        df = df.select(
            F.col("symbol"),
            F.col("datetime").cast(t.DateType()),
            F.col("open").cast(t.DoubleType()),
            F.col("high").cast(t.DoubleType()),
            F.col("low").cast(t.DoubleType()),
            F.col("close").cast(t.DoubleType()),
            F.col("dt_reference").cast(t.DateType()),
        )

        return df

    def clean_symbols(self, df: DataFrame) -> DataFrame:
        return df.withColumn("symbol", F.split(F.col("symbol"), "/USD")[0])

    def metric_columns(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("symbol").orderBy("datetime")

        df = (
            df.withColumn(
                "return_1d",
                (F.col("close") - F.lag("close").over(w)) / F.lag("close").over(w),
            )
            .withColumn("pct_change", (F.col("close") - F.col("open")) / F.col("open"))
            .withColumn("range", (F.col("high") - F.col("close")) / F.col("open"))
        )

        df = df.filter(F.col("datetime") <= self.date)

        return df

    def save(self, df: DataFrame) -> DataFrame:
        self.writer.write_data(
            df,
            WriterConfig(
                iceberg_table_cfg=IcebergTableConfig(
                    table_name=self.table.full_name(),
                    schema=self.table.schema(),
                    partitions=["symbol", "datetime"],
                ),
                mode=EnumIngestionMode.UPSERT,
                strategy=EnumMergeStrategy.TYPE1,
                merge_columns=["symbol", "datetime"],
            ),
        )

    def run(self) -> None:
        crypto_df = self.get_data()
        crypto_df = self.cast_columns(crypto_df)
        crypto_df = self.clean_symbols(crypto_df)
        crypto_df = self.metric_columns(crypto_df)
        self.save(crypto_df)

        crypto_df.show()
