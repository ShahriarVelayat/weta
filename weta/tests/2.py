#%%
# Import SparkSession
from pyspark.sql import SparkSession

# Build the SparkSession
spark = SparkSession.builder \
   .master("local") \
   .appName("Linear Regression Model") \
   .config("spark.executor.memory", "1gb") \
   .getOrCreate()
   
sc = spark.sparkContext


#%%

logFile = "/Users/Chao/spark-2.2.0-bin-hadoop2.7/README.md"  # Should be some file on your system
logData = sc.textFile(logFile).toDF(['text']).cache()

print(type(logData))

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))


#%%
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/Users/Chao/cars.csv')

#%%
spark.stop()