import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType



maxHits = 100

def getRemoteHost(line):
        fields = line.split("-")
        return fields[0]

def getDateAndTime(line):
        fields = line.split("-")
        return fields[3]

spark = SparkSession.builder.appName("ApacehAccessSparkKafka") \
        .getOrCreate()

schema = StructType([
  StructField('ipaddress', StringType(), True),
  StructField('c1', StringType(), True),
  StructField('date', StringType(), True),
  StructField('systeminfo', StringType(), True)
])

batchDataframe = spark.readStream.format("kafka")\
        .schema(schema) \
        .option("subscribe", "accesslog") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .load()

selectedDataframe = batchDataframe.select("ipaddress")

calucatedDataframe = batchDataframe.groupBy("ipaddress").count("ipaddress").alis("noOfHit")

filterdatabasedonnoofhits = calucatedDataframe.filter(calucatedDataframe.noOfHits > maxHits)

writtenDataFrame = filterdatabasedonnoofhits.writeStream \
      .save("/tmp/output").start()

writtenDataFrame.awaitTermination()







