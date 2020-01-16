from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
df=spark.read.csv('filename.csv',header=True)
df.createOrReplaceTempView("blabla")
df.show()
sqlDF = spark.sql("SELECT * FROM blabla")
sqlDF.write.csv('mycsv.csv')
