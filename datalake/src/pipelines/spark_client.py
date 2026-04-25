import os


from pyspark.sql import SparkSession


class SparkClient:
    _spark: SparkSession = None

    def __init__(self, app_name: str, warehouse: str):
        self.app_name = app_name
        self.warehouse = warehouse

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


spark_client = SparkClient(
    app_name="financial_pipeline",
    warehouse=os.getenv(
        "LAKEHOUSE_PATH",
        "/home/arthur/Arthur/Projetos/financial_pipeline/lakehouse",
    ),
)
