from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import udf, col, array,expr,var_pop
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType
import numpy as np
from typing import List, Tuple
import time


def get_spark_context(on_server):
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
        
        # Set number of cores to utilize
        # spark_conf.set("spark.executor.instances", "4")

        # # Enable to store the event log
        # spark_conf.set("spark.eventLog.enabled", "true")
        # #Location where to store event log
        # spark_conf.set("spark.eventLog.dir", r"C:\Program Files\Spark\spark-3.2.0-bin-hadoop3.2\tmp\spark-events")
        # #Location from where history server to read event log
        # spark_conf.set("history.fs.logDirectory", r"C:\Program Files\Spark\spark-3.2.0-bin-hadoop3.2\tmp\spark-events")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("WARN")

    return spark_context


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    # vectors_file_path = "/vectors.csv" if on_server else "40vectors_q3.csv"
    vectors_file_path = "/vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\10vectors.csv" 

    spark_session = SparkSession(spark_context)

    # df = spark_session.read.csv(vectors_file_path)
    # columns = ['id', 'vector']
    # df = df.toDF(*columns)

    # rdd = df.rdd

    rdd = spark_context.textFile(vectors_file_path,5)
    def parse_line(line):
        parts = line.split(',')
        id = parts[0]
        vector = parts[1]      
        return Row(id=id, vector=vector)
    rdd = rdd.map(parse_line)

    if on_server:
        # rdd = rdd.repartition(5)
        print('\nrdd.getNumPartitions()', rdd.getNumPartitions(), '\n')
    else:
        # rdd = rdd.repartition(2)
        print('\nrdd.getNumPartitions()', rdd.getNumPartitions(), '\n')

    return rdd


def q3(spark_context: SparkContext, rdd: RDD):

    # rdd.toDF().show()
    print('started running Q3')
    
    def agg_vector_var(v1: str, v2: str, v3: str) -> int:
        "Creates aggregate of three vector strings and returns its variance"

        v1_array = np.fromstring(v1, dtype=int, sep=";")
        v2_array = np.fromstring(v2, dtype=int, sep=";")
        v3_array = np.fromstring(v3, dtype=int, sep=";")

        triplet_agg = np.add(v1_array, v2_array)
        triplet_agg = np.add(triplet_agg, v3_array)
        triplet_var = np.var(triplet_agg)

        return triplet_var

    print('\nrdd.getNumPartitions()', rdd.getNumPartitions(), '\n')

    id_rdd = rdd.map(lambda x: x[0])
    double_id_rdd = id_rdd.cartesian(id_rdd).filter(lambda x: x[0] < x[1])
    triple_id_rdd = double_id_rdd.cartesian(id_rdd).filter(lambda x: x[0][1] < x[1])
    tau = 410
    broadcasted_rdd = spark_context.broadcast(rdd.collectAsMap()) #broad cast the rdd to all nodes
    triplets_var = triple_id_rdd.map(lambda x: (x[0][0], x[0][1], x[1], agg_vector_var(broadcasted_rdd.value[x[0][0]], broadcasted_rdd.value[x[0][1]], broadcasted_rdd.value[x[1]])))\
        .filter(lambda x: x[3] <= tau)
    print("there are ", triplets_var.count(), "triplets with variance less than ", tau)
    clct = triplets_var.collect()
    for i in range(0, min(20,len(clct))):
        print(clct[i])


    # # Compute all triplets
    # triplets = rdd.cartesian(rdd).cartesian(rdd) \
    #         .map(lambda x: (x[0][0], x[0][1], x[1])) \
    #         .filter(lambda x: x[0] < x[1] and x[1] < x[2]) #.cache()
    
    # triplets = triplets.repartition(4)
        
    # # Compute aggregate vector variance filter based on tau threshold
    # tau = 1000.0
    # triplets_var = triplets.map(lambda x: (x[0][0], x[1][0], x[2][0], agg_vector_var(x[0][1], x[1][1], x[2][1]))) \
    #                     .filter(lambda x: x[3] <= tau) #.cache()
    # # print('Tau:', tau, 'Count', triplets_var.count())

    # clct = triplets_var.collect()
    # print(clct)

    # print('\ntriplets_var.getNumPartitions()', triplets_var.getNumPartitions(), '\n')

    # print('Pr')

    # time.sleep(3600)


if __name__ == '__main__':

    on_server = True  # Set this to true if and only if deploying to the server
    if not on_server:
        import findspark
        findspark.init()

    spark_context = get_spark_context(on_server)

    rdd = q1b(spark_context, on_server)

    q3(spark_context, rdd)

    spark_context.stop()