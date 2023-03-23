from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType
from pyspark.sql.functions import col, array,expr,var_pop
from pyspark.sql.functions import udf
import numpy as np
def get_spark_context(on_server) -> SparkContext:
    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info. For larger data sets, to not set the
        # log level to anything below WARN, Spark will print too much information.
        spark_context.setLogLevel("WARN")

    return spark_context


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    vectors_file_path = "/vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\vectors.csv" 

    spark_session = SparkSession(spark_context)
    rdd = spark_context.textFile(vectors_file_path,minPartitions=5)#, minPartitions=16
    if not on_server:
        rdd = spark_context.textFile(vectors_file_path)

    def parse_line(line):
        parts = line.split(',')
        name = parts[0]
        vector = [int(x) for x in parts[1].split(';')]      
        return Row(name=name, vector=vector)

    parsed_rdd = rdd.map(parse_line)
    df = spark_session.createDataFrame(parsed_rdd)
    # print("The number of partitions is",df.rdd.getNumPartitions())
    # df.show()
    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\vectors.csv" 

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    return None

def q2(spark_context: SparkContext, data_frame: DataFrame):
    spark = SparkSession(spark_context)
    # tick = time.time()
    data_frame = data_frame.cache()
    @udf(returnType=FloatType())
    def aggregate_variance(vectors):
        vectors = np.array(vectors)
        aggregate_vector = vectors.sum(axis=0)
        variance = np.var(aggregate_vector)
        return float(variance)
    # @udf(returnType=FloatType())
    # def aggregate_variance(aggregate_vector):
    #     # print("calculating variance")
    #     # print("The aggregate vector is",aggregate_vector)
    #     # Calculate the mean of the aggregate vector
    #     mean = sum(aggregate_vector) / 20
        
    #     # Calculate the variance of the aggregate vector
    #     variance = sum([(x - mean) ** 2 for x in aggregate_vector]) / 20
        
    #     return variance


    # Get a table containing all aggregate vectors
    # triple_vectors_df = data_frame.select("Name", "Vector").withColumnRenamed("Name", "X").withColumnRenamed("Vector", "X_vector").crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Y").withColumnRenamed("Vector", "Y_vector")).crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Z").withColumnRenamed("Vector", "Z_vector")).filter("X < Y AND Y < Z")
    result_df1 = data_frame.select("Name", "Vector").withColumnRenamed("Name", "X").withColumnRenamed("Vector", "X_vector").crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Y").withColumnRenamed("Vector", "Y_vector")).filter("X < Y")
    result_df2 = result_df1.crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Z").withColumnRenamed("Vector", "Z_vector")).filter("Y < Z")
    # print("The number of result_df2 partitions is",result_df2.rdd.getNumPartitions()) 
    # result_df2.show()
    print("The cross join has been done")
    # result_df.show()
    # result_df = result_df.withColumn("agg_vector", expr("transform(arrays_zip(X_vector, Y_vector,Z_vector), x -> x.X_vector + x.Y_vector + x.Z_vector)")).select("X", "Y", "Z", "agg_vector")
    # result_df.show()
    result_df3 = result_df2.withColumn("var", aggregate_variance(array("X_vector", "Y_vector", "Z_vector")))
    # print("The number of result_df3 partitions is",result_df3.rdd.getNumPartitions())    
    # result_df = result_df.withColumn("var", aggregate_variance(col("agg_vector")))
    print("The variance has been calculated")
    # result_df.show()
    # result_df = result_df.select("X", "Y", "Z", "var")
    # result_df.show()
    # print("Line numbers are",result_df.count())
    
    # print("The dataframe is being split")
    # result_df = result_df.randomSplit([0.1, 0.9])[0]
    # print("The dataframe has been split")
    # print("There are",result_df.count(),"rows in the dataframe")
    result_df4 = result_df3.filter(col("var") <= 410)
    # print("The number of result_df4 partitions is",result_df4.rdd.getNumPartitions())
    print("The variance has been filtered")
    # result_df4.show()
    result_df5 = result_df4.select("X", "Y", "Z", "var")
    # print("The number of partitions is",result_df5.rdd.getNumPartitions())
    print("there are ",result_df5.count(),"rows in the dataframe")
    result_df5.show() # no need to show the dataframe as specified in the assignment
    # print("The time taken is",time.time()-tick)

def q3(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q3 here
    return


def q4(spark_context: SparkContext, rdd: RDD):
    # TODO: Imlement Q4 here
    return


if __name__ == '__main__':

    on_server = True  # TODO: Set this to true if and only if deploying to the server
    if not on_server:
        import findspark
        import time
        findspark.init()    

    spark_context = get_spark_context(on_server)

    data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    q2(spark_context, data_frame)

    q3(spark_context, rdd)

    q4(spark_context, rdd)

    spark_context.stop()
