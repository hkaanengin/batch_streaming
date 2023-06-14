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

# spark = SparkSession.builder.getOrCreate()

# parqDF=spark.read.parquet("measurements.parquet")
# parqDF.createOrReplaceTempView("TempHum")
# parqDF.printSchema()
# parqDF.show(truncate=False)

# pd=parqDF.toPandas()
# print(len(pd))
# print(type(pd.iat[0,0]))
# print(type(pd.iat[0,1]))
# print(type(pd.iat[0,2]))
# print(type(pd.iat[0,3]))
