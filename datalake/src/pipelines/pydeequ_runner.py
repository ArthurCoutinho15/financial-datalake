from networkx import check_planarity
from pyspark.sql import SparkSession, DataFrame
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

from pipelines.models import DeequConfig


class DeequRunner:
    def __init__(self, spark: SparkSession) -> VerificationSuite:
        self._spark = spark

    def run_checks(self, df: DataFrame, config: DeequConfig):
        check = Check(self._spark, CheckLevel.Error, "Data Quality Check")

        for rule in config.checks:
            if rule.check_type == "completeness":
                check = check.isComplete(rule.column)
            elif rule.check_type == "uniqueness":
                check = check.isUnique(rule.column)
            elif rule.check_type == "min":
                check = check.hasMin(rule.column, lambda x: x >= rule.value)
            elif rule.check_type == "max":
                check = check.hasMin(rule.column, lambda x: x <= rule.value)

        result = VerificationSuite(self._spark).onData(df).addCheck(check).run()

        if result.status != "Success":
            raise Exception("Data Quality failed")

        return result
