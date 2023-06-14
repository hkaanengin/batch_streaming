import pyspark
from pyspark.sql import SparkSession

def Parq_to_pandas():
            
    spark = SparkSession.builder.getOrCreate()

    parqDF=spark.read.parquet("measurements.parquet")
    # parqDF.createOrReplaceTempView("TempHum")
    # parqDF.printSchema()
    # parqDF.show(truncate=False)

    pd=parqDF.toPandas()
    return pd 
