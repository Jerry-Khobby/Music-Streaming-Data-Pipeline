class GlueContext:
    def __init__(self, spark_context):
        self._sc = spark_context

    @property
    def spark_session(self):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    class _DynamicFrameFactory:
        def from_catalog(self, database, table_name):
            from awsglue.dynamicframe import DynamicFrame
            return DynamicFrame()

    @property
    def create_dynamic_frame(self):
        return self._DynamicFrameFactory()
