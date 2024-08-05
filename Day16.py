# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

print('hello')

# COMMAND ----------

df_01=spark.read.csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_01.show

# COMMAND ----------

df_01.display()

# COMMAND ----------

df_01.count()

# COMMAND ----------

df_01.printSchema()

# COMMAND ----------

df_02=spark.read.option("header",True).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_02.show()

# COMMAND ----------

df_02.display()

# COMMAND ----------

df_02.printSchema()

# COMMAND ----------

df_03=spark.read.option("header",True).option("inferschema",True).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df_03.display()

# COMMAND ----------

df_03.printSchema()

# COMMAND ----------

#for large data we should use user defind schema


# COMMAND ----------

sch=StructType().add("App",StringType(),True)\
    .add("Category",StringType(),True)\
    .add("Rating",DoubleType(),True)\
    .add("Reviews",IntegerType(),True)\
    .add("Size",StringType(),True)\
    .add("Installs",StringType(),True)\
    .add("Type",StringType(),True)\
    .add("Price",StringType(),True)\
    .add("Content Rating",StringType(),True)\
    .add("Genres",StringType(),True)\
    .add("Last Updated",StringType(),True)\
    .add("Current Ver",StringType(),True)\
    .add("Android Ver",StringType(),True)


# COMMAND ----------

df4=spark.read.option("header",True).schema(sch).csv("/FileStore/gmde/googleplaystore.csv")

# COMMAND ----------

df4.display()

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

df5=df4.withColumn("app_gener",concat(col("App"),col("Genres")))

# COMMAND ----------

df5.display()

# COMMAND ----------

df6=df5.withColumn("App_Name",col("App"))

# COMMAND ----------

df6.display()

# COMMAND ----------

df7=df6.withColumn("multi",col("Reviews")*col("Rating"))

# COMMAND ----------

df7.display()

# COMMAND ----------

df8=df7.withColumn("owner",lit("harun"))

# COMMAND ----------

df8.display()

# COMMAND ----------

#we can do this in single command is called chaining transformation

# COMMAND ----------

df9=df8.withColumn("owner2",lit("ramesh")).withColumn("multi2",col("Reviews")*col("Rating"))

# COMMAND ----------

df9.display()

# COMMAND ----------

df10=df9.withColumnRenamed("App","App_Name")

# COMMAND ----------

df10.display()

# COMMAND ----------

df11=df10.select("Rating")

# COMMAND ----------

df12=df10.selectExpr('cast(Rating as integer) as new_rating')

# COMMAND ----------

df12.display()

# COMMAND ----------

#aliasing

# COMMAND ----------

df_sort=df10.sort(col("Rating"))

# COMMAND ----------

df_sort.display()

# COMMAND ----------

df_sort_desc=df10.sort(col("Rating").desc())


# COMMAND ----------

df_sort_desc.display()

# COMMAND ----------

df10.count()

# COMMAND ----------

df_distinct=df10.distinct()


# COMMAND ----------

df_distinct.count()

# COMMAND ----------

#drop duplicate for perticular columns
df_dropduplicate= df10.dropDuplicates(["Category","Rating"])

# COMMAND ----------

df_dropduplicate.count()

# COMMAND ----------

# case statement
new_rating = df_dropduplicate.withColumn(
    "Rating",
    when((col("Rating") > 4.3) & (col("Rating") < 4.5), "Average")
    .when((col("Rating") > 4.5) & (col("Rating") < 4.8), "Good")
    .when(col("Rating") > 4.8, "Excellent")
    .otherwise("not upto the mark")
)

# COMMAND ----------

new_rating.display()

# COMMAND ----------

df_reviews=df_01=spark.read.csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df_reviews.display()

# COMMAND ----------

df_reviews.printSchema()

# COMMAND ----------

df_reviews.count()

# COMMAND ----------

sch2=StructType().add("App",StringType(),True)\
    .add("Translated_Review",StringType(),True)\
    .add("Sentiment",StringType(),True)\
    .add("Sentiment_Polarity",IntegerType(),True)\
    .add("Sentiment_Subjectivity",StringType(),True)

# COMMAND ----------

df_reviews1=spark.read.option("header",True).schema(sch2).csv("/FileStore/gmde/googleplaystore_user_reviews.csv")

# COMMAND ----------

df_joined=df4.join(df_reviews1,df4.App==df_reviews1.App,how="left").select(df4['*'],df_reviews1["Sentiment"])

# COMMAND ----------

df_joined.display()

# COMMAND ----------

grouped=df_joined.groupby("App").avg("Rating")

# COMMAND ----------

grouped.display()

# COMMAND ----------


