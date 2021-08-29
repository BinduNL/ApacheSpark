from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
rawDF = spark.read.json("ipl_json", multiLine = "true")
innings=rawDF.select(explode("innings").alias("innings"))
DF1=innings.select(col("innings.team"),explode("innings.overs.deliveries").alias("batbowl"))
DF2=DF1.select(col("batbowl.batter").alias("batter"),col("batbowl.bowler").alias("bowler"))
DF3=DF2.select(col("batter"),col("bowler"))
DF4=DF3.withColumn("batter",arrays_zip("batter")).select("bowler",explode("batter").alias("merge")).select("bowler",col("merge.batter"))
total_runs=innings.withColumn("deliveries",explode("innings.overs.deliveries")).withColumn("runs",explode("deliveries.runs")).select("runs.total")
df1=DF4.withColumn("id",monotonically_increasing_id())
df2=total_runs.withColumn("id",monotonically_increasing_id())
df3=df2.join(df1,"id","outer").drop("id")
df4=df3.groupBy("batter","bowler").agg(sum("total"))
df5=df4.orderBy("batter",decending=False)
DF5=df5.withColumn("result",array_distinct("bowler"))
DF5.show(400,False)

