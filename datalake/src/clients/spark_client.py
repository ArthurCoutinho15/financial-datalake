from pyspark.sql import SparkSession


class SparkClient:
    _spark = None

    def __init__(self, app_name: str, warehouse: str):
        self.app_name = app_name
        self.warehouse = f"file:///{warehouse}"

    def get_session(self):

        if SparkClient._spark is None:
            SparkClient._spark = (
                SparkSession.builder.appName(self.app_name)
                .config(
                    "spark.jars.packages",
                    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0",
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


spark_client = SparkClient(
    app_name="financial_pipeline",
    warehouse="/home/arthur/Arthur/Projetos/financial_pipeline/datalake/lakehouse",
)
