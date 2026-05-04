from typing import Optional, List

from pyspark.sql import DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as f

from pipelines.models.writer import WriterConfig, EnumIngestionMode, EnumMergeStrategy
from .spark_client import spark_client


class SparkWriter:
    def __init__(self):
        self._spark = spark_client.get_session()

    def _spark_to_sql_type(self, data_type: t.DataType) -> str:
        if isinstance(data_type, t.StringType):
            return "STRING"
        elif isinstance(data_type, t.DoubleType):
            return "DOUBLE"
        elif isinstance(data_type, t.LongType):
            return "LONG"
        elif isinstance(data_type, t.IntegerType):
            return "INT"
        elif isinstance(data_type, t.LongType):
            return "BIGINT"
        elif isinstance(data_type, t.TimestampType):
            return "TIMESTAMP"
        elif isinstance(data_type, t.DateType):
            return "DATE"
        elif isinstance(data_type, t.BooleanType):
            return "BOOLEAN"
        else:
            raise ValueError(f"Tipo não suportado: {data_type}")

    def create_iceberg_table(
        self, table_name: str, schema: t.StructType, partitions: Optional[List[str]]
    ):
        columns = []

        for field in schema.fields:
            col_name = field.name
            col_type = self._spark_to_sql_type(field.dataType)
            nullable = "" if field.nullable else "NOT NULL"

            columns.append(f"{col_name} {col_type} {nullable}".strip())

        columns_sql = ",\n".join(columns)

        partition_sql = ""
        if partitions:
            partition_sql = f"PARTITIONED BY ({', '.join(partitions)})"

        query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns_sql}
            )
            USING iceberg
            {partition_sql}
        """
        self._spark.sql(query)

    def _merge_data(
        self,
        table_name: str,
        df: DataFrame,
        merge_columns: list[str],
    ) -> None:
        """
        Generic merge (upsert) for Iceberg tables

        Merge strategy for data ingestion
        - If ID already exixts: UPDATE
        - If ID is new: INSERT
        """

        if not merge_columns:
            raise ValueError("merge_columns is mandatory in UPSERT")

        df = df.dropDuplicates(merge_columns)

        df.createOrReplaceTempView("staging")

        on_conditions = " AND ".join(
            [f"target.{col} = source.{col}" for col in merge_columns]
        )

        columns = df.columns

        update_set = ",\n".join([f"{col} = source.{col}" for col in columns])

        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        self._spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING staging AS source
            ON {on_conditions}

            WHEN MATCHED THEN UPDATE SET
            {update_set}

            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """)

    def _merge_type2(
        self,
        table_name: str,
        df: DataFrame,
        business_keys: list[str],
        compare_columns: list[str],
    ) -> None:
        """
        SCD Type 2 merge for Iceberg tables

        - Closes old registries whenever there is a change
        - Insert a new version with is_current = true
        """

        if not business_keys or not compare_columns:
            raise ValueError("merge_columns is mandatory in UPSERT")

        df = df.dropDuplicates(business_keys)

        df.createOrReplaceTempView("staging")

        join_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in business_keys]
        )

        change_condition = " OR ".join(
            [
                f"coalesce(target.{col}, '') <> coalesce(source.{col}, '')"
                for col in compare_columns
            ]
        )

        self._spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING staging AS source
            ON {join_condition} AND target.is_current = true

            WHEN MATCHED AND ({change_condition}) THEN UPDATE SET
                target.valid_to = current_timestamp(),
                target.is_current = false
        """)

        # add columns SCD2 into DF
        df_new = (
            df.withColumn("valid_from", f.current_timestamp())
            .withColumn("valid_to", f.lit(None).cast("timestamp"))
            .withColumn("is_current", f.lit(True))
        )

        df_new.createOrReplaceTempView("staging_new")

        columns = df_new.columns

        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        self._spark.sql(f"""
            INSERT INTO {table_name}
            SELECT {insert_values}
            FROM staging_new source
            LEFT JOIN {table_name} target
            ON {join_condition} AND target.is_current = true
            WHERE target.{business_keys[0]} IS NULL
            OR ({change_condition})
        """)

    def _append_data(self, table_name: str, df: DataFrame) -> None:
        """
        Generic append for Iceberg tables
        """

        if df.isEmpty():
            return

        df.writeTo(table_name).append()

    def write_data(self, df: DataFrame, writer_config: WriterConfig) -> None:
        self.create_iceberg_table(
            table_name=writer_config.iceberg_table_cfg.table_name,
            schema=writer_config.iceberg_table_cfg.schema,
            partitions=writer_config.iceberg_table_cfg.partitions,
        )

        if writer_config.mode == EnumIngestionMode.UPSERT:
            if writer_config.strategy == EnumMergeStrategy.TYPE1:
                self._merge_data(
                    writer_config.iceberg_table_cfg.table_name,
                    df,
                    merge_columns=writer_config.merge_columns,
                )
            elif writer_config.strategy == EnumMergeStrategy.TYPE2:
                business_keys = writer_config.business_keys
                compare_columns = writer_config.compare_columns

                self._merge_type2(
                    writer_config.iceberg_table_cfg.table_name, df, business_keys, compare_columns
                )
            else:
                raise ValueError(
                    f"Invalid strategy for UPSERT: {writer_config.strategy}"
                )

        elif writer_config.mode == EnumIngestionMode.APPEND:
            self._append_data(writer_config.iceberg_table_cfg.table_name, df)
        else:
            raise ValueError(f"Invalid write mode: {writer_config.mode}")
