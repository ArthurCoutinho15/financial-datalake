import os

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t


class SparkClient:
    _spark: SparkSession = None

    def __init__(self, app_name: str, warehouse: str):
        self.app_name = app_name
        self.warehouse = warehouse

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
            rest_catalog_uri = os.getenv("ICEBERG_REST_URI", "http://localhost:8182")
            SparkClient._spark = (
                SparkSession.builder.appName(self.app_name)
                .config(
                    "spark.jars",
                    ",".join(
                        [
                            "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar",
                            "/opt/spark/jars/iceberg-aws-bundle-1.6.0.jar",
                            "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                            "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
                        ]
                    ),
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                )
                .config(
                    "spark.sql.catalog.hadoop_catalog",
                    "org.apache.iceberg.spark.SparkCatalog",
                )
                .config("spark.sql.catalog.hadoop_catalog.type", "rest")
                .config("spark.sql.catalog.hadoop_catalog.uri", rest_catalog_uri)
                .config("spark.sql.default.catalog", "hadoop_catalog")
                # S3 configs
                .config(
                    "spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")
                )
                .config(
                    "spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")
                )
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                .config(
                    "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
                .config("spark.hadoop.fs.s3a.path.style.access", "false")
                .getOrCreate()
            )

        return SparkClient._spark

    def merge_data(
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

        df.createOrReplaceTempView("staging")

        on_conditions = " AND ".join(
            [f"target.{col} = source.{col}" for col in merge_columns]
        )

        columns = df.columns

        update_set = ",\n".join([f"{col} = source.{col}" for col in columns])

        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        self.get_session().sql(f"""
            MERGE INTO {table_name} AS target
            USING staging AS source
            ON {on_conditions}

            WHEN MATCHED THEN UPDATE SET
            {update_set}

            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
        """)

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
