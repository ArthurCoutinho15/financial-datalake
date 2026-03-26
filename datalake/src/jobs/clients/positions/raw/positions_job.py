from datetime import date

from pyspark.sql import DataFrame, SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window

from .positions_table import RawPositionsTable
from clients.spark_client import spark_client


class RawPositionsJob:
    def __init__(self, date: date):
        self.date =  date
        self.table = RawPositionsTable()
        self.path = "/home/arthur/Arthur/Projetos/financial_pipeline/datalake/src/seeds/fake_data_positions.csv"
        self.spark: SparkSession = spark_client.get_session()
        
    def get_data(self) -> DataFrame:
        df = self.spark.read.csv(self.path, header=True, schema=self.table.schema())
        
        return df
    
    def create_dt_reference_column(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn(
                "dt_reference",
                F.lit(self.date)
            )
        )
    
    def save(self, df: DataFrame) -> None:
        spark_client.create_iceberg_table(
            table_name=self.table.full_name(),
            schema=self.table.schema(),
            partitions=["dt_reference"]
        )
        
        df.writeTo(self.table.full_name()).overwritePartitions()
    
    def run(self):
        df = self.get_data()
        df = self.create_dt_reference_column(df)
        self.save(df)
        
        df.show()