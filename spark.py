from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField,ArrayType,LongType,StringType,DoubleType,IntegerType
from pyspark.sql.functions import split, explode,col

spark = SparkSession.builder.appName("sparkss").getOrCreate()
'''spark-submit --driver-class-path /usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar spark.py'''

df = spark.read.json('/home/saurav/Documents/Fusemachines/Kafka_Project/data.json')

product_schemas = StructType([StructField('code', LongType(), True), StructField('data', ArrayType(StructType([StructField('categories', ArrayType(LongType(), True), True), 
StructField('description', StringType(), True), StructField('ean', StringType(), True), StructField('id', LongType(), True), 
StructField('image', StringType(), True), StructField('images', ArrayType(StructType([StructField('description', StringType(), True), 
StructField('title', StringType(), True), StructField('url', StringType(), True)]), True), True), StructField('name', StringType(), True), 
StructField('net_price', DoubleType(), True), StructField('price', StringType(), True), StructField('tags', ArrayType(StringType(), True), True), 
StructField('taxes', LongType(), True), StructField('upc', StringType(), True)]), True), True), StructField('status', StringType(), True), 
StructField('total', LongType(), True)])

# df = spark.readStream.schema(product_schemas).json("/home/saurav/Documents/Fusemachines/Kafka_Project/data.json")
df.printSchema()


# df_product = df.select(F.from_json(F.col("value"), product_schemas).alias(
#     "data")).select("data.*")

df1=df.select("*", explode("data").alias("exploded_data"))

df2 = df1.select("*",F.col("exploded_data.*")).drop("code","status","total","data","exploded_data")

df2.printSchema()
df2.show()





#### TRANSFORMATION 1####
categories_explode = df2.select(df2['id'],df2['name'],df2['categories']).withColumn('categories',F.explode(col='categories'))
categories_explode.show()

categories_explode.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Transform2', user='fusemachines', password='hello123').mode('overwrite').save()


#### TRANSFORMATION 2 ####

float_df = df2.select(df2['id'],df2['price'],df2['net_price'])

int_df = float_df. \
    withColumn("price",float_df['price'].cast(IntegerType())) \
    .withColumn("net_price",float_df['net_price'].cast(IntegerType()))

int_df.show()

int_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Transform3', user='fusemachines', password='hello123').mode('overwrite').save()


#### TRANSFORMATION 3 #####

select_column = df2.select(df2['id'],df2['name'],df2['price'],df2['taxes'])
column_renamed = select_column.withColumnRenamed("taxes","Taxes(in %)").withColumnRenamed("price","price(in NRS)")
column_renamed.show()

column_renamed.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Transform4', user='fusemachines', password='hello123').mode('overwrite').save()







##### TRANSFORMATION 1#####
avg_df=df2.agg({'net_price':'avg'})
avg_df.show()

avg_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Transform1', user='fusemachines', password='hello123').mode('overwrite').save()