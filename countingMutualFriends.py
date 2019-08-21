"""
# import spark environment
"""
import os
execfile(os.path.join(os.environ["SPARK_HOME"], 'python/pyspark/shell.py'))


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, collect_list, size, col, row_number,sort_array
from pyspark.sql import Window

sparkSession = SparkSession.builder.enableHiveSupport().master("local [2]").getOrCreate()

# Read Graph data 
graphPath = "/data/graphDFSample"
reversedGraph = sparkSession.read.parquet(graphPath)
#reversedGraph.printSchema()


filt_frnd_ul = reversedGraph.withColumn("friend",explode("friends")).groupBy("friend").agg(collect_list("user").alias("user_list")).filter(size("user_list")>1)


sorted_userlist = filt_frnd_ul.select(sort_array("user_list").alias("users"))


# Create UDF for emitting pairs
from pyspark.sql.types import ArrayType,IntegerType
from pyspark.sql.functions import udf
def emit_pairs(array):
    alen = len(array)
    ans = []
    for i in xrange(alen):
        for j in xrange(i+1,alen):
            ans.append([array[i],array[j]])
    return ans
pair_udf = udf(lambda x:emit_pairs(x),ArrayType(ArrayType(IntegerType())))

ans = sorted_userlist.select(pair_udf("users").alias("pairs")).withColumn("pair",explode("pairs")) 
ans = ans.select(ans["pair"].getItem(0).alias("fir1"),ans["pair"].getItem(1).alias("fir2")).repartition(100)

ans.createTempView("answer")
sparkSession.sql("select a,b,count(1) as cnt from (select pair[0] as a ,pair[1] as b from answer) as ll group by a,b order by cnt ").show(10)

