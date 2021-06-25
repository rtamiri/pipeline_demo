from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col

SparkContext.setSystemProperty('spark.executor.memory', '8g')
sc = SparkContext("local", "Spark_Enron")
spark = SparkSession \
    .builder \
    .appName("Spark_Enron") \
    .getOrCreate()
# loading
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("./export/*.csv")
print(df.columns)

Q1_df = df.filter(col("id").contains("JavaMail")).groupBy("id").agg(countDistinct("id"))
Q1_file = open("Q1.txt", "w")
Q1_file.write(str(Q1_df.count()))
Q1_file.close()
Q2_df = df.filter(
    col("to").contains("@")) \
    .groupBy('to') \
    .agg(countDistinct("id")) \
    .filter("to is NOT null AND to != ''") \
    .sort(col("count(id)").desc())
Q2_df.show()
Q2_df.repartition(1) \
    .write.format("com.databricks.spark.csv") \
    .option("header", "true") \
    .save("Q2.txt")
empDF.alias("emp1").join(empDF.alias("emp2"), \
                         col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
    .select(col("emp1.emp_id"), col("emp1.name"), \
            col("emp2.emp_id").alias("superior_emp_id"), \
            col("emp2.name").alias("superior_emp_name")) \
    .show(truncate=False)
Q3_df = df.alias("df1").join(df.alias("df2"), col("df1.from") == col("df2.to") & col("df1.id") == col("df2.id")) \
    .select(col("df1.from"), col("df1.to"), col(df1.
"df1.subject"), col("df1.date")) \
    .withColumn("time_for_reply", col("df2.date") - col("df1.date")) \
    .agg(Distinct("id")) \
 \
df.createOrReplaceTempView("q3_view")
spark.sql(
    "CREATE TEMPORARY VIEW q3_view_int AS SELECT first(m1.from) as from,first(m1.to) as to, first(m1.subject) as subject, first(m1.date) as date, min(m2.date) as response_date" +
    " FROM q3_view m1 JOIN q3_view m2 ON m1.from = m2.to AND to_timestamp(m2.date) > to_timestamp(m1.date)" +
    " GROUP BY m1.from, m1.to, m1.date")

spark.sql("SELECT * from q3_view_int").show()

spark.sql(
    "CREATE TEMPORARY VIEW q3_view_int2 AS SELECT from, to , subject, date, to_timestamp(response_date) - to_timestamp(date) as time_for_reply" +
    " from q3_view_int")

spark.sql(" SELECT from, to , subject, date,time_for_reply " +
          " from q3_view_int where time_for_reply is NOT NULL ORDER BY time_for_reply asc").show()
