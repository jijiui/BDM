import os
import sys
from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, sum, variance
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
os.system("set PYSPARK_PYTHON=python")
import findspark
findspark.init()


def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("ERROR")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    vectors_file_path = "vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\vectors.csv" 

    spark_session = SparkSession(spark_context)

    # TODOdone: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.
    df = spark_session.read.option("inferSchema",True).\
        csv(vectors_file_path)
    print(df)
    df.show()
    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\vectors.csv" 

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    rdd = spark_context.textFile(vectors_file_path) \
        .map(lambda line: [float(x) for x in line.split(',')])
    #dataColl=rdd.collect()
    # for row in dataColl:
    #     print(row[0] + "," +str(row[1]))

    return rdd

def get_string(n):
    result = []
    for i in range(1, n + 1):
        for j in range(1, 4):
            result.append(f"v{j}._c{i}")
    return ", ".join(result)

def get_sum(x):
    s = 0
    for i in x:
        s = s + i
    return s

def q2(spark_context: SparkContext, data_frame: DataFrame):
    spark = SparkSession(spark_context)
    def aggregate_variance(*data):
        # Split data into three vectors
        n = 3 #lenth of vector
        v1 = data[:n]
        v2 = data[n:2*n]
        v3 = data[2*n:]

        # Calculate aggregate vector
        aggregate_vector = [get_sum(x) for x in zip(v1,v2,v3)]
        # Calculate mean
        mean = get_sum(aggregate_vector)/n

        # Calculate aggregate variance
        s = 0
        for x in aggregate_vector:            
            s = s + (x-mean)**2

        aggregate_variance = s/n

        return aggregate_variance

    spark.udf.register("agg_variance", aggregate_variance, FloatType())

    data_frame.createOrReplaceTempView("vectors")

    #triples_df = spark.sql("SELECT v1._c0 AS X, v2._c0 AS Y, v3._c0 AS Z, agg_variance(v1._c1, v2._c1, v3._c1, v1._c2, v2._c2, v3._c2, v1._c3, v2._c3, v3._c3) AS var FROM vectors v1, vectors v2, vectors v3 WHERE v1._c0 < v2._c0 AND v2._c0 < v3._c0")
    triples_df = spark.sql("SELECT v1._c0 AS X, v2._c0 AS Y, v3._c0 AS Z, agg_variance("+get_string(3)+") AS var FROM vectors v1, vectors v2, vectors v3 WHERE v1._c0 < v2._c0 AND v2._c0 < v3._c0")

    result_df = triples_df.filter("var <= 1000")
    result_df.show()

def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q3 here
    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return


if __name__ == '__main__':

    on_server = False  # TODO: Set this to true if and only if deploying to the server

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
