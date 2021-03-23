import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
#Connect to the cluster
# New API
spark = SparkSession\
        .builder\
        .master("spark://192.168.2.77:7077") \
        .config("spark.dynamicAllocation.executorIdleTimeout","30s")\
        .config("spark.executor.cores",2)\
        .appName("pa_test1")\
        .getOrCreate()

# Old API (RDD)
sc = spark.sparkContext

sc.setLogLevel("WARN")

nlines = sc.textFile("hdfs://192.168.2.77:9000/user/ubuntu/negative-words.txt")
plines = sc.textFile("hdfs://192.168.2.77:9000/user/ubuntu/positive-words.txt")

df = spark.read.json("hdfs://192.168.2.77:9000/user/ubuntu/RC_2010-01.bz2")
data_clean = df.select("subreddit", "body", "score", "controversiality", "created_utc").filter(df["subreddit"].like("%linux%"))



def compile_regexp(word_list):
    re_string = "[\s\W]("
    for word in word_list:
        re_string += (re.escape(word) + "|")
    re_string = re_string[0:-1] + ")[\s\W]"
    return re.compile(re_string, re.IGNORECASE)

negative = compile_regexp(nlines.collect())
sc.broadcast(negative)
positive = compile_regexp(plines.collect())
sc.broadcast(positive)


def match_negative(comment, wc):
    return len(negative.findall(comment))/wc

def match_positive(comment, wc):
    return len(positive.findall(comment))/wc

def count_words(comment):
    return len(comment.split())

udf_match_negative = udf(match_negative, DoubleType())
udf_match_positive = udf(match_positive, DoubleType())
udf_count_words = udf(count_words, IntegerType())

w_count = data_clean.withColumn('wordcount', udf_count_words('body'))
negativity = w_count.withColumn('negativity', udf_match_negative('body', 'wordcount'))
n_p_df = negativity.withColumn('positivity', udf_match_positive('body', 'wordcount'))
filtered = n_p_df.select("subreddit", "score", "controversiality", "wordcount", "negativity", "positivity", "created_utc").filter("positivity != 0 and negativity != 0")
df1 = filtered.groupBy("subreddit", floor(filtered.created_utc/3600)).avg("positivity", "negativity")


df2 = df1.withColumn("Happines Ratio", df1["avg(positivity)"] / df1["avg(negativity)"]).cache()
df2.write.mode('append').option("header", "true").csv("hdfs://192.168.2.77:9000/user/ubuntu/test_output")
