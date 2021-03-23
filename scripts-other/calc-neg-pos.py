import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# New API
spark = SparkSession\
        .builder\
        .master("spark://192.168.2.207:7077") \
        .config("spark.dynamicAllocation.executorIdleTimeout","30s")\
        .config("spark.executor.cores",2)\
        .appName("pa_test")\
        .getOrCreate()

# Old API (RDD)
sc = spark.sparkContext



nlines = sc.textFile("hdfs://192.168.2.207:9000/user/ubuntu/negative-words.txt")
plines = sc.textFile("hdfs://192.168.2.207:9000/user/ubuntu/positive-words.txt")

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

df = spark.read.json("hdfs://192.168.2.207:9000/user/ubuntu/sample_data.json")
data_clean = df.select("subreddit", "body", "score", "controversiality")


def match_negative(comment):
    return len(negative.findall(comment))

def match_positive(comment):
    return len(positive.findall(comment))

udf_match_negative = udf(match_negative, StringType())
udf_match_positive = udf(match_positive, StringType())

negativity = data_clean.withColumn('negativity', udf_match_negative('body').cast('Int'))
n_p_df = negativity.withColumn('positivity', udf_match_positive('body').cast('Int'))
n_p_df.show()

