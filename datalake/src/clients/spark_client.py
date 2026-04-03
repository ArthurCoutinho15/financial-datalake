import os

from pyspark.sql import SparkSession
import pyspark.sql.types as t


class SparkClient:
    _spark: SparkSession = None

    def __init__(self, app_name: str, warehouse: str):
        self.app_name = app_name
        self.warehouse = f"file:///{warehouse}"

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

    def get_session(self):
        if SparkClient._spark is None:
            SparkClient._spark = (
                SparkSession.builder.appName(self.app_name)
                .config(
                    "spark.jars",
                    "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar",
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(
                    "spark.sql.catalog.hadoop_catalog",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.hadoop_catalog.warehouse", self.warehouse)
                .config("spark.sql.default.catalog", "hadoop_catalog")
                .getOrCreate()
            )

        return SparkClient._spark

    def create_iceberg_table(
        self, table_name: str, schema: t.StructType, partitions: list[str]
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


spark_client = SparkClient(
    app_name="financial_pipeline",
    warehouse=os.getenv(
        "LAKEHOUSE_PATH",
        "/home/arthur/Arthur/Projetos/financial_pipeline/lakehouse",
    ),
)
