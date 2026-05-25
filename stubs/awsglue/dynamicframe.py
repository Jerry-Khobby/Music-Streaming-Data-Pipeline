class DynamicFrame:
    def toDF(self):
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame([], schema=[])
