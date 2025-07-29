

from pyspark.sql import SparkSession
from pyspark.sql.functions import mean,median,mode


spark =SparkSession.builder.appName("titanicDataClean").getOrCreate()

df=spark.read.csv("/home/jd/BigDataAnalytics/datasets/titanic.csv",header=True,inferSchema=True)
df = df.na.fill({"Age": df.select(mean("Age")).first()[0]})
df = df.dropDuplicates()
df.show(5)