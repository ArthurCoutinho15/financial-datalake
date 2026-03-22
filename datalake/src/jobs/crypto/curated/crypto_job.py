from datetime import date

from pyspark.sql import DataFrame, SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window

from .crypto_table import CuratedCryptoTable
from ..raw.crypto_table import RawCryptoTable
from clients.spark_client import spark_client


class CuratedCryptoJob:
    def __init__(self, date: date = date.today()):
        self.date = date
        self.source = RawCryptoTable()
        self.table = CuratedCryptoTable()
        self.spark: SparkSession = spark_client.get_session()
        
    
    def get_data(self) -> DataFrame:
        return (
            self.spark.read.table(self.source.full_name())
            .filter(
                F.col("dt_reference") == self.date
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
        return (
            df
            .withColumn("symbol", F.split(F.col("symbol"), "/USD")[0])
        )
    
    def metric_columns(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("symbol").orderBy("datetime")
        
        df = (
            df
            .withColumn("return_1d", (F.col("close") - F.lag("close").over(w)) / F.lag("close").over(w))
            .withColumn("pct_change", (F.col("close") - F.col("open")) / F.col("open"))
            .withColumn("range", (F.col("high") - F.col("close")) / F.col("open"))
        )
        
        return df
    
    def save(self, df: DataFrame) -> DataFrame:
        spark_client.create_iceberg_table(
            table_name=self.table.full_name(),
            schema=self.table.schema(),
            partitions=["dt_reference"]
        )
        df.writeTo(self.table.full_name()).overwritePartitions()
        
    def run(self) -> None:
        crypto_df = self.get_data()
        crypto_df = self.cast_columns(crypto_df)
        crypto_df = self.metric_columns(crypto_df)
        self.save(crypto_df)
        
        crypto_df.show()
    
    