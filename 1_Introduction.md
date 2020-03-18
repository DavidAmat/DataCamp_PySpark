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



# 1. Data Manipulation

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
# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()
```

## Grouping

```python
# Group by variable
by_plane = flights.groupBy("tailnum")

# Count occurrences per group
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# AVG on a variable
by_origin.avg("air_time").show()
```

## Grouping and Aggregating - Functions

```python
# Import pyspark.sql.functions as F
import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy("month","dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation of departure delay
by_month_dest.agg(F.stddev("dep_delay")).show()
```

## Join

```python
# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on = "dest", how = "leftouter")

# Examine the new DataFrame
print(flights_with_airports.show())
```


- Start from the "planes" dataframe:
```sql
-- planes
--/root
 |-- tailnum: string (nullable = true)
 |-- year: string (nullable = true)
 |-- type: string (nullable = true)
 |-- manufacturer: string (nullable = true)
 |-- model: string (nullable = true)
 |-- engines: string (nullable = true)
 |-- seats: string (nullable = true)
 |-- speed: string (nullable = true)
 |-- engine: string (nullable = true)

```
```sql
-- flights
 |-- year: string (nullable = true)
 |-- month: string (nullable = true)
 |-- day: string (nullable = true)
 |-- dep_time: string (nullable = true)
 |-- dep_delay: string (nullable = true)
 |-- arr_time: string (nullable = true)
 |-- arr_delay: string (nullable = true)
 |-- carrier: string (nullable = true)
 |-- tailnum: string (nullable = true)
 |-- flight: string (nullable = true)
 |-- origin: string (nullable = true)
 |-- dest: string (nullable = true)
 |-- air_time: string (nullable = true)
 |-- distance: string (nullable = true)
 |-- hour: string (nullable = true)
 |-- minute: string (nullable = true)
```

```python
# Rename year column
planes = planes.withColumnRenamed("year", "plane_year")

# Join the DataFrames
model_data = flights.join(planes, on="tailnum", how="leftouter")
```

# 2. Machine Learning Pipelines

## Data types

 That means all of the columns in your DataFrame must be either integers or decimals (called 'doubles' in Spark).
 Unfortunately, Spark doesn't always guess right and you can see that some of the columns in our DataFrame are strings containing numbers as opposed to actual numeric values.

 ### Cast
 The only argument you need to pass to .cast() is the kind of value you want to create, in string form. For example, to create integers, you'll pass the argument "integer" and for decimal numbers you'll use "double". Cast only gets column data, not dataframe. 

```python
# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time", model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month", model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))

# Create new column
# Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)

# Create boolean
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# Convert boolean to an integer
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))
```
## Remove missing values

```python
# Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")
```

## Modelling Pipeline

### Strings and factors - One Hot Encoding

In pyspark.ml.features:

```python
# Create a StringIndexer
carr_indexer = StringIndexer(inputCol="carrier", outputCol="carrier_index")
# Create a OneHotEncoder
carr_encoder = OneHotEncoder(inputCol="carrier_index", outputCol="carrier_fact")

# Create a StringIndexer
dest_indexer = StringIndexer(inputCol="dest",outputCol="dest_index")
# Create a OneHotEncoder
dest_encoder = OneHotEncoder(inputCol="dest_index",outputCol="dest_fact")
```
### VectorAssembler

The last step in the Pipeline is to combine all of the columns containing our features into a single column. This has to be done **before modeling can take place because every Spark modeling routine expects the data to be in this form**. You can do this by storing each of the values from a column as an entry in a vector. Then, from the model's point of view, **every observation is a vector that contains all of the information** about it and a **label that tells the modeler what value that observation corresponds to**.

```python
# Make a VectorAssembler and combine all of these columns into an assembler called "features"
vec_assembler = VectorAssembler(inputCols=["month", "air_time", "carrier_fact", "dest_fact", "plane_age"], outputCol="features")
```

### Pipeline
Pipeline is a class in the pyspark.ml module that combines all the Estimators and Transformers that you've already created. This lets you reuse the same modeling process over and over again by wrapping it up in one simple object.


```python
# Import Pipeline
from pyspark.ml import Pipeline

# Make the pipeline
flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])
```

#### Fit and transform

```python
# Fit and transform the data
piped_data = flights_pipe.fit(model_data).transform(model_data)
```

#### Split data randomSplit
```python
# Split the data into training and test sets
training, test = piped_data.randomSplit([.6, .4])
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