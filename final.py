# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import calendar
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Final") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()


from dotenv import load_dotenv
import os

load_dotenv()

pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PW")





# %% [markdown]
# ### Load CSV then Data Cleaning

# %%
df1 = spark.read.csv("/home/rojesh/Documents/finalSpark/Fuel_Station_Information.csv", header=True, inferSchema=True)
df2 = spark.read.csv("/home/rojesh/Documents/finalSpark/Hourly_Gasoline_Prices.csv", header=True, inferSchema=True)

joined_df = df1.join(df2, "Id", "inner")

cleaned_df = joined_df.dropna()

# Data Cleaning:Removing rows where the "Type" column has the value "autostradle"
cleaned_df = cleaned_df.filter(F.col("Type") != "Autostradle")

# %% [markdown]
# ### Saving the cleaned file as parquet file

# %%
parquet_path = "../trying/parquet"
cleaned_df.coalesce(6).write.parquet(parquet_path, mode="overwrite")

# %% [markdown]
# ### Connecting Spark to Dbeaver and writing the cleaned csv into the Postgres

# %%

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

table_name = "newtable"



cleaned_df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

# %% [markdown]
# ### Reading the file from Postgres and Working in it for our queries

# %%


# Define the JDBC connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

table_name = "newtable"

#filteringtable
filter_condition = "1=1 LIMIT 10000"

df3 = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"(SELECT * FROM {table_name} WHERE {filter_condition}) AS filtered_table") \
    .options(**properties) \
    .load()


row_count = df3.count()
print(f"Number of rows read: {row_count}") 


df3.show()

# %% [markdown]
# ### Calculate the average seasonal prices for a dataset containing date and price information, while also assigning each date a season label based on the month then writing the final table into Postgres

# %%
df = df3.withColumn('month', F.month('date')) \
        .withColumn('Season', F.when(F.col('month').between(3, 5), 'Spring')
                               .when(F.col('month').between(6, 8), 'Summer')
                               .when(F.col('month').between(9, 11), 'Autumn')
                               .otherwise('Winter'))

window_spec = Window.partitionBy('Season')

seasonal_avg_prices = df.withColumn('Average Seasonal Price',
                                    F.avg('Price').over(window_spec))

seasonal_avg_prices = seasonal_avg_prices.dropDuplicates(['Season'])

seasonal_avg_prices = seasonal_avg_prices.orderBy(F.col("Average Seasonal Price").desc())

seasonal_avg_prices.select("Season", "Average Seasonal Price").show()


jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

table_name = "Seasontable"

columns_to_insert = ("Season","Average Seasonal Price")

seasonal_avg_prices.select(*columns_to_insert).write.jdbc(url=jdbc_url, table=table_name, mode="Overwrite", properties=properties)



# %% [markdown]
# ###  Find the distance between the locations with the minimum and maximum prices in a dataset then Writing thr resulting table into Postgres

# %%
min_price_row = df3.orderBy(F.col("Price")).first()
max_price_row = df3.orderBy(F.col("Price").desc()).first()

print(min_price_row)
print(max_price_row)

#checking if min_price_row and max_price_row are not None
if min_price_row is not None and max_price_row is not None:
    min_latitude = float(min_price_row["Latitude"])
    min_longitude = float(min_price_row["Longitudine"])
    max_latitude = float(max_price_row["Latitude"])
    max_longitude = float(max_price_row["Longitudine"])

    min_latitude_rad = F.radians(F.lit(min_latitude)).cast("double")
    min_longitude_rad = F.radians(F.lit(min_longitude)).cast("double")
    max_latitude_rad = F.radians(F.lit(max_latitude)).cast("double")
    max_longitude_rad = F.radians(F.lit(max_longitude)).cast("double")

    distance_km = F.acos(
    F.sin(min_latitude_rad) * F.sin(max_latitude_rad) +
    F.cos(min_latitude_rad) * F.cos(max_latitude_rad) *
    F.cos(max_longitude_rad - min_longitude_rad)
    ).cast("double") * 6371.0

    df_with_distance = df3.withColumn("Distance_km", distance_km)
    df_with_distance.select("Distance_km").distinct().show()
else:
    print("No data found to calculate minimum and maximum prices.")

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user":pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

table_name = "Distance"



df_with_distance.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)




# %% [markdown]
# ### Calculate the average prices for each day of the month then presents the results in a pivot table then Writing the resulting table into Postgres
# 

# %%
df = df3.withColumn("day_of_month", F.dayofmonth("Date"))
df = df.withColumn("month", F.month("Date"))


day_pivot_table = df.groupBy("day_of_month").pivot("month").agg(F.avg("Price"))

# Define a list of month names
month_names = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]


for i in range(1, 13):
    month_name = month_names[i - 1]
    day_pivot_table = day_pivot_table.withColumnRenamed(str(i), month_name)


day_pivot_table = day_pivot_table.fillna(0)

day_pivot_table.show()

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

table_name = "question3"


day_pivot_table.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=properties)


