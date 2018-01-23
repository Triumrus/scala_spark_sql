# scala_spark_sql
# начало стандартные команды для выполнение задач на спарк sql

%spark
val df = spark.read
    .options(Map("header"->"false","sep"->"\t"))
    .csv("hdfs:///EDW_UAT/staging/landing/001/0005/000097/0107/001_0005_000097_A_20170501000000_20170531235959_0107_0000_PSTN_CALLS_00_MMYYYY.csv.bz2",
    "hdfs:///EDW_UAT/staging/landing/001/0005/000097/0107/001_0005_000097_A_20170516000000_20170531235959_0107_0000_PSTN_CALLS_00_MMYYYY.csv.bz2"
    )

df.createOrReplaceTempView("logi")


%spark
df.show()
%spark
df.printSchema()
%spark
df.select($"_c2".as("login")).show

%spark
val query = """
SELECT 
a._c4
FROM logi a
left join logi2 b
on a._c4 = b._c4
where a._c4 = '1042'
LIMIT 10
"""


val result = spark.sql(query)
result.show()
// result.coalesce(1).write
//     .options(Map("header"->"true","sep"->";"))
//     .csv("hdfs://hadoop/tmp/kuzmichev")



