from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession

import pyspark.sql.functions as F
import pyspark.sql.types as t
from pyspark.sql.window import Window

from .clients_table import CuratedClientsTable
from ..raw.clients_table import RawClientTable

from ....clients.portfolios.raw.portfolios_table import RawPortfoliosTable
from ....clients.positions.raw.positions_table import RawPositionsTable
from ....clients.transactions.raw.transaction_table import RawTransactionsTable

from clients.spark_client import spark_client


class CuratedClientsJob:
    def __init__(self, date: date):
        self.date = date
        self.source = {
            "clients": RawClientTable(),
            "portfolios": RawPortfoliosTable(),
            "positions": RawPositionsTable(),
            "transactions": RawTransactionsTable(),
        }
        self.table = CuratedClientsTable()
        self.spark = spark_client.get_session()

    def _get_clients_data(self) -> DataFrame:
        return self.spark.read.table(self.source["clients"].full_name()).filter(
            F.col("dt_reference") == F.lit(self.date)
        )

    def _get_portfolios_data(self) -> DataFrame:
        return self.spark.read.table(self.source["portfolios"].full_name()).filter(
            F.col("dt_reference") == F.lit(self.date)
        )

    def _get_positions_data(self) -> DataFrame:
        return self.spark.read.table(self.source["positions"].full_name()).filter(
            F.col("dt_reference") == F.lit(self.date)
        )

    def _get_transactions_data(self) -> DataFrame:
        return self.spark.read.table(self.source["transactions"].full_name()).filter(
            F.col("dt_reference") == F.lit(self.date)
        )

    def join_tables(self) -> DataFrame:
        clients = self._get_clients_data()
        portfolios = self._get_portfolios_data()
        positions = self._get_positions_data()
        transactions = self._get_transactions_data()

        final_df = (
            clients.alias("c")
            .join(
                portfolios.alias("p"),
                on=F.col("c.id") == F.col("p.client_id"),
                how="left",
            )
            .join(
                positions.alias("pos"),
                on=F.col("p.id") == F.col("pos.portfolio_id"),
                how="left",
            )
            .join(
                transactions.alias("t"),
                on=F.col("pos.id") == F.col("t.position_id"),
                how="left",
            )
            .select(
                F.col("c.id").alias("client_id"),
                F.col("c.name"),
                F.col("c.email"),
                F.col("c.cpf"),
                F.col("c.phone"),
                F.col("c.city"),
                F.col("c.state"),
                F.col("p.id").alias("portfolio_id"),
                F.col("p.name").alias("portfolio_name"),
                F.col("pos.id").alias("position_id"),
                F.col("pos.ticker"),
                F.col("pos.asset_type"),
                F.col("pos.quantity").alias("position_quantity"),
                F.col("pos.avg_price_brl").alias("position_avg_price_brl"),
                F.col("t.id").alias("transaction_id"),
                F.upper(F.col("t.type")).alias("transaction_type"),
                F.col("t.quantity").alias("transaction_quantity"),
                F.col("t.price_brl").alias("transaction_price_brl"),
                F.col("t.executed_at").alias("dt_transaction"),
            )
        )

        return final_df
    
    def cast_columns(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("position_quantity", F.col("position_quantity").cast(t.DoubleType()))
            .withColumn("position_avg_price_brl", F.col("position_avg_price_brl").cast(t.DoubleType()))
            .withColumn("transaction_quantity", F.col("transaction_quantity").cast(t.DoubleType()))
            .withColumn("transaction_price_brl", F.col("transaction_price_brl").cast(t.DoubleType()))
            .withColumn("dt_transaction", F.col("dt_transaction").cast(t.DateType()))
        )

    def create_dt_reference_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("dt_reference", F.lit(self.date))

    def save(self, df: DataFrame) -> None:
        spark_client.create_iceberg_table(
            table_name=self.table.full_name(),
            schema=self.table.schema(),
            partitions=["dt_reference"],
        )

        df.writeTo(self.table.full_name()).overwritePartitions()

    def run(self) -> None:

        df = self.join_tables()
        df = self.cast_columns(df)
        df = self.create_dt_reference_column(df)
        self.save(df)

        print(df.columns)
