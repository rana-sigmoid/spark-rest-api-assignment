import glob

import pyspark
import pandas as pd
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("SparkByExamples.com").getOrCreate()

import requests

url = "https://stock-market-data.p.rapidapi.com/market/index/s-and-p-six-hundred"

headers = {
	"X-RapidAPI-Key": "40b27a5d62mshead33450c6e50ccp159aeejsn1841eea50cff",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

response = requests.request("GET", url, headers=headers)

print(response.text)

stock_names = ["ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS", "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH", "AIN", "AIR", "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX"]
query1={}
df_stocks = None
for stock_n in stock_names:

    df3 = spark.read.option("header", "true").csv(f"stock_data/{stock_n}.csv", inferSchema=True)
    df3= df3.withColumn("stock_name", lit(stock_n))
    df3=df3.withColumn("Stock_moved", (((col("Close")-col("Open"))/col("Open"))*100))

    if df_stocks is None:
        df_stocks = df3
    else:
        df_stocks = df_stocks.union(df3)

# print(df_stocks.count())
#df_stocks.show(5431)
df_stocks.createOrReplaceTempView("data")

# query 1

spark.sql("create temporary view temp1 as (SELECT Date, max(stock_moved) as positive from data group by Date) ")
spark.sql("create temporary view temp2 as (SELECT Date, min(stock_moved) as negative from data group by Date) ")
spark.sql("create temporary view temp3 as (SELECT Date, stock_name as max_stock, "
          "(stock_moved) as positive from data where stock_moved in (select positive from temp1 where "
          "data.Date=temp1.Date)) ")
spark.sql("create temporary view temp4 as (SELECT Date, stock_name as min_stock, "
          "(stock_moved) as negative from data where stock_moved in (select negative from temp2 where "
          "data.Date=temp2.Date)) ")
spark.sql("select temp3.Date, max_stock, positive, min_stock, negative from temp3 inner join "
      "temp4 on temp3.Date = temp4.Date")


# query 2
query2_data = spark.sql("SELECT Date, stock_name, Volume from data where Date || Volume in "
                            " (SELECT Date || Max(Volume) from data GROUP BY Date) Order BY Date")


# query 3
spark.sql("select stock_name, Date, Open, Close, LAG(Close,1,0) over ( "
          "partition by stock_name order by Date) as previous_close from data ASC").show()
spark.sql("with added_previous_close as (select stock_name, Date, Open, Close, LAG(Close,1,0) over ( "
          "partition by stock_name order by Date) as previous_close from data ASC) select stock_name, "
          " ABS(Open-previous_close) as max_swing from added_previous_close order by max_swing DESC limit 1").show()


# query 4
#spark.sql("select stock_name, Date as mindate, Open, stock_name from data where mindate in (select min(Date) from data group by stock_name").show()
spark.sql("create temporary view temp11 as (select Date, Open, stock_name from data where stock_name || Date in ( select  "
          "stock_name || min(Date) from data group by stock_name))")

spark.sql("create temporary view temp12 as (select Date, High, stock_name from data where stock_name || High in ( "
          "select stock_name || max(High) from data group by stock_name))")

spark.sql("create temporary view query4 as (select temp11.stock_name, (temp12.High-temp11.Open) as maximum_moved "
          "from temp11 inner join temp12 on temp11.stock_name=temp12.stock_name)")
spark.sql("select * from query4 where maximum_moved in (select max(maximum_moved) from query4)")


# query 5
spark.sql("select stock_name, STDDEV(Volume) from data group by stock_name")

# query 6
spark.sql("select stock_name, avg(Volume) as mean, percentile_approx(Volume, 0.5) as Median from data group by stock_name")

# query 7
spark.sql("select stock_name, avg(Volume) from data group by stock_name")
# query 8
#spark.sql("select stock_name from data where max(Volume) in (select stock_name, avg(Volume) from data group by stock_name)").show()
# spark.sql("select stock_name from data group by stock_name having "
#           "avg(volume) = select max(avg(Volume)) from data group by stock_name").show()
spark.sql("create temporary view temp8 as (select stock_name, avg(Volume) AS avg_volume from data group by stock_name)")
spark.sql("select stock_name from temp8 where avg_volume = (select max(avg_volume) from temp8)").show()

# query 9
spark.sql("SELECT stock_name, max(high) as highest_price, min(low) as lowest_price from data GROUP BY stock_name")


# where temp1.Date LEFT JOIN data on data.Date= data.Date").show(4444)
#spark.sql("SELECT * from data").show(4444)









#df3 = spark.read.option("delimiter", ",").option("header", "true").csv("/Users/ranadilendrasingh/PycharmProjects/python-spark-assignment/stock_data/ABCB.csv")

# use_cols = ['Open', 'Close', 'Date']
# df3 = pd.read_csv("/Users/ranadilendrasingh/PycharmProjects/python-spark-assignment/stock_data/ABCB.csv", usecols=lambda x: x in use_cols,
#                   index_col=False)
# #df3.printSchema()
# print(df3)
# # df_copy = df3.iloc[:, [1,4,7]]
# # df_copy.printSchema()
#spark.sql("SELECT Date, moved, stock_name from data Group By Date").show(4444)

#spark.sql("SELECT stock_name from data").show(4444)

#spark.sql("SELECT count(*) from data").show()
# result_pdf = df3.select("*").toPandas()



#df3.show(5432)
# print(query)



# list of columns that we want to
# read into the DataFrame
# use_cols = ['Open', 'Close', 'Date']
#
# # Reading the csv file
# df = pd.read_csv("/Users/ranadilendrasingh/PycharmProjects/python-spark-assignment/stock_data/ABCB.csv", usecols=lambda x: x in use_cols,
#                  index_col=False)
#
# # Print the dataframe
# print(df)
# query= spark.sql("SELECT Date from df")
# print(query)
#print(df3.head(5))
#
#df_copy = df[['Name', 'Number', 'College']]