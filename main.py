from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType
from pyspark.sql.functions import col, array,expr,var_pop
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


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
    vectors_file_path = "/vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\vectors_10000.csv" 

    spark_session = SparkSession(spark_context)
    # url = spark_session.sparkContext.uiWebUrl
    # print("Spark UI running on {}".format(url))
    # TODOdone: Implement Q1a here by creating a Dataset of DataFrame out of the file at {@code vectors_file_path}.
    schema = StructType([
        StructField("Name", StringType(), True),
        *[StructField(f"int_column_{i}", IntegerType(), True) for i in range(1, 10001)]
    ])

    # Read the CSV file with the specified schema
    df = spark_session.read \
        .option("header", False) \
        .option("sep", ",") \
        .schema(schema) \
        .csv(vectors_file_path)
    #df.show()
    print("The csv file has been read into a dataframe")
    cols_to_select = [col_name for col_name in df.columns[1:]]
    df = df.select(col("Name"), array(*cols_to_select).alias("Vector"))
    # df.show()
    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    vectors_file_path = "/vectors.csv" if on_server else r"C:\Users\handa\OneDrive - TU Eindhoven\Q7\BDM\2AMD15 Data Generator\vectors.csv" 

    # TODO: Implement Q1b here by creating an RDD out of the file at {@code vectors_file_path}.

    return None

def q2(spark_context: SparkContext, data_frame: DataFrame):
    spark = SparkSession(spark_context)
    
    @udf(returnType=FloatType())
    def calc_variance(aggregate_vector):
        # Calculate the mean of the aggregate vector
        mean = sum(aggregate_vector) / 3
        
        # Calculate the variance of the aggregate vector
        variance = sum([(x - mean) ** 2 for x in aggregate_vector]) / 3
        
        return variance
    # Get a table containing all aggregate vectors
    # triple_vectors_df = data_frame.select("Name", "Vector").withColumnRenamed("Name", "X").withColumnRenamed("Vector", "X_vector").crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Y").withColumnRenamed("Vector", "Y_vector")).crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Z").withColumnRenamed("Vector", "Z_vector")).filter("X < Y AND Y < Z")
    result_df = data_frame.select("Name", "Vector").withColumnRenamed("Name", "X").withColumnRenamed("Vector", "X_vector").crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Y").withColumnRenamed("Vector", "Y_vector")).filter("X < Y")
    result_df = result_df.crossJoin(data_frame.select("Name", "Vector").withColumnRenamed("Name", "Z").withColumnRenamed("Vector", "Z_vector")).filter("Y < Z")
    print("The cross join has been done")
    # result_df.show()
    result_df = result_df.withColumn("agg_vector", expr("transform(arrays_zip(X_vector, Y_vector,Z_vector), x -> x.X_vector + x.Y_vector + x.Z_vector)")).select("X", "Y", "Z", "agg_vector")
    print("The aggregate vectors have been calculated")
    #result_df.show()

    # agg_vectors_df.createOrReplaceTempView("agg_vectors")
    result_df = result_df.withColumn("var", calc_variance(col("agg_vector")))
    print("The variance has been calculated")
    #result_df.show()
    result_df = result_df.select("X", "Y", "Z", "var")
    result_df.show()
    #print("Line numbers are",result_df.count())
    #result_df = result_df.filter(col("var") <= 410)
    #print("The variance has been filtered")
    #result_df.show() # cannot show the filtered result

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
