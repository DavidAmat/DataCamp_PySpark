# DataCamp_PySpark
Spark is a platform for cluster computing. Spark lets you spread data and computations over clusters with multiple nodes (think of each node as a separate computer). Splitting up your data makes it easier to work with very large datasets because each node only works with a small amount of data.  As each node works on its own subset of the total data, it also carries out a part of the total calculations required, so that both data processing and computation are performed in parallel over the nodes in the cluster. It is a fact that parallel computation can make certain types of programming tasks much faster.  However, with greater computing power comes greater complexity.  Deciding whether or not Spark is the best solution for your problem takes some experience, but you can consider questions like:  Is my data too big to work with on a single machine? Can my calculations be easily parallelized?

The first step in using Spark is connecting to a cluster.

In practice, the cluster will be hosted on a remote machine that's connected to all other nodes. There will be one computer, called the master that manages splitting up the data and the computations. The master is connected to the rest of the computers in the cluster, which are called worker. The master sends the workers data and calculations to run, and they send their results back to the master.

When you're just getting started with Spark it's simpler to just run a cluster locally. Thus, for this course, instead of connecting to another computer, all computations will be run on DataCamp's servers in a simulated cluster.

Creating the connection is as simple as creating an instance of the SparkContext class. The class constructor takes a few optional arguments that allow you to specify the attributes of the cluster you're connecting to.

An object holding all these attributes can be created with the SparkConf() constructor. Take a look at the documentation for all the details!

For the rest of this course you'll have a SparkContext called sc already available in your workspace.

<img src="imgs\img0.png" width="600px" />

## Install

setx SPARK_HOME D:\spark\spark-3.0.0-preview2-bin-hadoop2.7
setx HADOOP_HOME D:\spark\spark-3.0.0-preview2-bin-hadoop2.7
setx PYSPARK_DRIVER_PYTHON jupyter
setx PYSPARK_DRIVER_PYTHON_OPTS notebook

Add to path: D:\spark\spark-3.0.0-preview2-bin-hadoop2.7\bin

## Connection

```python
# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

```

## Difference Spark Context and Session

Prior Spark 2.0, Spark Context was the entry point of any spark application and used to access all spark features and needed a sparkConf which had all the cluster configs and parameters to create a Spark Context object. We could primarily create just RDDs using Spark Context and we had to create specific spark contexts for any other spark interactions. For SQL SQLContext, hive HiveContext, streaming Streaming Application. In a nutshell, Spark session is a combination of all these different contexts. Internally, Spark session creates a new SparkContext for all the operations and also all the above-mentioned contexts can be accessed using the SparkSession object. (https://medium.com/@achilleus/spark-session-10d0d66d1d24)

## Basics reading and checking data

### Reading
- Parquet 
```python
df = my_spark.read.load(r'<path>.parquet')
```

- CSV

```python
df = spark.read.load(r"data/airports.csv",
                     format="csv", sep=",", inferSchema="true", header="true")
```

- JSON

```python
path = r'data/people.json'
peopleDF = spark.read.json(path)
```

### Creating Dataframes from Pandas

- From JSON string:
```python
jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeople = spark.read.json(otherPeopleRDD)
otherPeople.show()
```

- From Pandas DF:
```python
xx = pd.DataFrame([[1,2,3],[2,3,4],[3,4,5]], columns = ["A", "B","C"])
spark_df = sqlContext.createDataFrame(xx)
```

```python

```

### Basics
- Collect:

```python
df.select("col1", "col2").collect()
```
- Print Schema

```python
df.printSchema()
```

### Catalog

- List tables:

```python
spark.catalog.listTables()
```

- Create a table in Catalog:
```python
df.createOrReplaceTempView("vuelos")
```

- Load a table from Catalog to Spark Dataframe:

```python
flights = spark.table("vuelos")
flights.show()
```

- Global View (not temporary)
```python
df.createGlobalTempView("people")
spark.newSession().sql("SELECT * FROM global_temp.people").show()
```

### toPandas

- Querying a table in Catalog and converting to Pandas:
```python
spark.sql("SELECT * FROM vuelos LIMIT 10").toPandas()
```

### select
- Select multiple columns and show:

```python
df.select(df['id'], df['ident']).show()
```

## Creating columns (withColumns)

- Create column "duration_hrs" from air_time column:
```python
flights = flights.withColumn("duration_hrs", flights.air_time/60)
```


## Filtering Data

- SQL syntax:
```python
# Filter flights by passing a string
long_flights1 = flights.filter("distance > 1000")
```

- Pandas syntax:
```python
# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)
```

## Select vs. With Column
The difference between .select() and .withColumn() methods is that .select() returns only the columns you specify, while .withColumn() returns all the columns of the DataFrame in addition to the one you defined. It's often a good idea to drop columns you don't need at the beginning of an operation so that you're not dragging around extra data as you're wrangling. In this case, you would use .select() and not .withColumn().

- Select and define masks as filters:

```python
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)
```

## Alias and selectExpr()

- Define an alias
```python
# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")
```
- Include the alias in the selection of columns without quotes:
```python
# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)
```
- Using selectExpr() notation we can do the same:
```python
# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")
```

## Aggregating

- Group by global:
```python
# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()
```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```
```python

```

```python

```

```python

```

```python

```

```python

```

```python

```