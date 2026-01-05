import os
from pyspark.sql import SparkSession
from pyspark import StorageLevel

from util import Config


class Base:
    def __init__(self, task: str, config_path: str, yyyymmdd: str) -> None:

        self.yyyymmdd = yyyymmdd
        self.config = Config(config_path)

        self.spark = (
            SparkSession.builder.appName("voice_detector") \
            .enableHiveSupport() \
            .getOrCreate()
        )

        self.footprint = os.path.join(self.config["footprint"], task)
        print(f">> footprint: {self.footprint}")

        self.spark.sparkContext.setLogLevel("INFO")

    def close(self):
        """stop spark"""
        self.spark.catalog.clearCache()
        self.spark.stop()
        print("Sucessfully closed spark app")

    def reproduce(self, dataframe, name=None, schema=False, footprint=True):

        if schema:
            dataframe.printSchema()

        dir_name = f"{name}.parquet"
        path = os.path.join(self.footprint, dir_name)

        if footprint:
            dataframe.write.mode("overwrite").parquet(path)
            return self.spark.read.parquet(path)

        return dataframe.persist(StorageLevel.MEMORY_AND_DISK)
