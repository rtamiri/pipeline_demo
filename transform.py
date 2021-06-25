import re
import os
from pyspark import StorageLevel
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, HiveContext, SparkSession, Window
from pyspark.sql.functions import lag, col,lead,to_timestamp,split,udf
from pyspark.sql.functions import StringType,count,regexp_replace,when,explode,trim
from os import listdir
import pandas as pd
import email


# stage = 'spark_init'
# spark = SparkSession.builder.appName(stage).enableHiveSupport().getOrCreate()
# sc = SparkContext("local", "stage App")

conf=SparkConf()
conf.set("spark.executor.memory", "10g")
conf.set("spark.driver.memory", "4g")
conf.set("spark.cores.max", "6")
# conf.set("spark.driver.extraClassPath",
#     driver_home+'/jdbc/postgresql-9.4-1201-jdbc41.jar:'\
#     +driver_home+'/jdbc/clickhouse-jdbc-0.1.52.jar:'\
#     +driver_home+'/mongo/mongo-spark-connector_2.11-2.2.3.jar:'\
#     +driver_home+'/mongo/mongo-java-driver-3.8.0.jar') 

sc = SparkContext.getOrCreate(conf)

spark = SQLContext(sc)

DIR = "/Users/ravitamiri/Downloads/maildir/"
# Read all the folders and subfolders in the data
loc = listdir(DIR)

for index,l in enumerate(loc):
      li = []
      print(l)
      for r, d, files in os.walk(DIR+ l+ "/"):
            if r[-1] != "/":
                  li.append(r +"/*")
      data_dir= sc.wholeTextFiles(",".join(li))
      data_dir = data_dir.toDF()
      data_dir = data_dir.dropDuplicates()
      if index == 0:
            data= data_dir
      else:
            data = data.union(data_dir)

# data = spark.read.format('parquet').options(headers = True).load('Downloads/enron')
data = data.withColumnRenamed("_1", "location")
data = data.withColumnRenamed("_2", "message")
data = data.withColumn("user" ,split(col("location"),"/").getItem(5))
data = data.withColumn("message" ,regexp_replace("message","N","_n"))


ToEmailParses = udf(lambda z: ToEmailParse(z), StringType())
spark.udf.register("ToEmailParse", ToEmailParses)

def ToEmailParse(s):
        msg = email.message_from_string(r'{}'.format(s))
        return msg['to']
data = data.withColumn('to',ToEmailParses('message'))


FromEmailParses = udf(lambda z: FromEmailParse(z), StringType())
spark.udf.register("FromEmailParse", FromEmailParses)

def FromEmailParse(s):
        msg = email.message_from_string(r'{}'.format(s))
        return msg['from']
data = data.withColumn('from',FromEmailParses('message'))


DateParses = udf(lambda z: ToEmailParse(z), StringType())
spark.udf.register("ToEmailParse", DateParses)

def ToEmailParse(s):
        msg = email.message_from_string(r'{}'.format(s))
        return msg['Date']
data = data.withColumn('Date',DateParses('message'))


SubjectParses = udf(lambda z: SubjectParse(z), StringType())
spark.udf.register("SubjectParse", SubjectParses)

def SubjectParse(s):
        msg = email.message_from_string(r'{}'.format(s))
        return msg['Subject']

data = data.withColumn('subject',SubjectParses('message'))

DateCorParses = udf(lambda z: DateCorParse(z), StringType())
spark.udf.register("DateCorParse", DateCorParses)

def DateCorParse(s):
       return "-".join(s.split(" ")[1:5])


data = data.withColumn('DateCor',DateCorParses('Date'))
data = data.withColumn('DateCor',to_timestamp("DateCor", "dd-MMM-yyyy-HH:mm:ss"))

data = data.withColumn("subject", regexp_replace("subject","\s+",""))
# data = data.withColumn("subject", regexp_replace("subject"," ",""))


# Question 1 Answer
# Get the count of the emails
data.select("location").distinct().count()

text_file = open("Q1.txt", "w")
text_file.write("%s" % int(data.select("location").distinct().count()))
text_file.close()

# Verified by calculating the number of files from os directory

data = data.select("*",(explode(split(col("to"), ",")).alias("to_clean"))).where('to_clean != ""')
data = data.withColumn("to_clean", regexp_replace("to_clean","\s+",""))
data = data.withColumn("to_clean", regexp_replace("to_clean"," ",""))





# Question 2
# Here we explode to to column as it is a comma separated list and then perform 
data_q2 = data.groupBy('to_clean').agg(count("message").alias("total_messages")).orderBy("total_messages",ascending=False)

data_q2 = data_q2.toPandas()
data_q2.to_csv("Q2.txt",index = False,sep = "\t")
# Verfied by looking at the volume of message for a person they seem to be inline 

# Question 3:
# Here we do a self join and try to match the from and to variable and then match the subject with "Re:" Removed to denote response
# Question is asked about millliseconds be the date is only time Seconds
data_join = data.select(['to','from','DateCor','subject','message'])
data_join = data_join.withColumnRenamed("to", "to_self")
data_join = data_join.withColumnRenamed("DateCor", "DateCor_self")
data_join = data_join.withColumnRenamed("message", "message_self")
data_join = data_join.withColumnRenamed("subject", "subject_self")
data_join = data_join.withColumnRenamed("from", "from_self")
data_join = data_join.where(col("to_self") != col("from_self"))
data_join = data_join.where(col("subject_self") != "")
data = data.withColumnRenamed("from", "from_")

data_join_df  = data.join(data_join, (data.from_ == data_join.to_self) & (data.to == data_join.from_self))
data_join_df.persist(StorageLevel.MEMORY_AND_DISK_2)
data_join_df.count()
diff_secs_col = col("DateCor_self").cast("long")-col("DateCor").cast("long") 
data_join_df = data_join_df.withColumn( "diff_secs", diff_secs_col )
data_join_df = data_join_df.withColumn( "subject_self", regexp_replace("subject_self","Re:","") )
data_join_df = data_join_df.where(col("subject") == col("subject_self"))
data_join_df = data_join_df.where(col("diff_secs") >= data_join_df.agg({"diff_secs": "min"}).collect()[0])

mail = data_join_df.where(col("diff_secs") == 0).collect()[0]
msg = email.message_from_string(r'{}'.format(mail['message']))

text_file = open("Q3.txt", "w")
text_file.write("%s\n" % msg['from'])
text_file.write("%s\n" % msg['to'])
text_file.write("%s\n" % msg['Subject'])
text_file.write("%s\n" % msg['Date'])
text_file.write("%s" % mail['diff_secs'])
text_file.close()

