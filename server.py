from flask import Flask
import json

from reading_data import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


# print(df_stocks.count())
#df_stocks.show(5431)




app = Flask(__name__)

@app.route('/maximum-moved-stock-daily', methods=['POST', 'GET'])
def query1():
    spark.sql("create temporary view temp1_1 as (SELECT Date, max(stock_moved) as positive from data group by Date) ")
    spark.sql("create temporary view temp1_2 as (SELECT Date, min(stock_moved) as negative from data group by Date) ")
    spark.sql("create temporary view temp1_3 as (SELECT Date, stock_name as max_stock, "
              "(stock_moved) as positive from data where stock_moved in (select positive from temp1_1 where "
              "data.Date=temp1_1.Date)) ")
    spark.sql("create temporary view temp1_4 as (SELECT Date, stock_name as min_stock, "
              "(stock_moved) as negative from data where stock_moved in (select negative from temp1_2 where "
              "data.Date=temp1_2.Date)) ")
    result = spark.sql("select temp1_3.Date, max_stock, positive, min_stock, negative from temp1_3 inner join "
              "temp1_4 on temp1_3.Date = temp1_4.Date")

    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/most-traded-stock', methods=['POST', 'GET'])
def query2():
    query2_data = spark.sql("SELECT Date, stock_name, Volume from data where Date || Volume in "
                            " (SELECT Date || Max(Volume) from data GROUP BY Date) Order BY Date")

    return json.loads(query2_data.toPandas().to_json(orient="table", index=False))


@app.route('/maxup-or-maxdown-opening-stock', methods=['POST', 'GET'])
def query3():
    result= spark.sql("with added_previous_close as (select stock_name, Date, Open, Close, LAG(Close,1,0) over ( "
          "partition by stock_name order by Date) as previous_close from data ASC) select stock_name, "
          " ABS(Open - previous_close) as max_swing from added_previous_close order by max_swing DESC limit 1 ")

    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/maximum-moved-stock', methods=['POST', 'GET'])
def query4():
    spark.sql(
        "create temporary view temp4_1 as (select Date, Open, stock_name from data where stock_name || Date in ( select  "
        "stock_name || min(Date) from data group by stock_name))")

    spark.sql("create temporary view temp4_2 as (select Date, High, stock_name from data where stock_name || High in ( "
              "select stock_name || max(High) from data group by stock_name))")

    spark.sql("create temporary view temp4_3 as (select temp4_1.stock_name, (temp4_2.High-temp11.Open) as "
              "maximum_moved from temp4_1 inner join temp4_2 on temp4_1.stock_name=temp4_2.stock_name)")
    result = spark.sql("select * from temp4_3 where maximum_moved in (select max(maximum_moved) from temp4_3)")

    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/standard-deviation-of-stock', methods=['POST', 'GET'])
def query5():
    result= spark.sql("select stock_name, STDDEV(Volume) from data group by stock_name")
    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/mean-median-of-stock', methods=['POST', 'GET'])
def query6():
    result= spark.sql("select stock_name, avg(Volume) as mean, percentile_approx(Volume, 0.5) as Median from data group by stock_name")
    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/average-volume-of-stock', methods=['POST', 'GET'])
def query7():
    result= spark.sql("select stock_name, avg(Volume) from data group by stock_name")
    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/highest-average-volume-stock', methods=['POST', 'GET'])
def query8():
    spark.sql("create temporary view temp8_1 as (select stock_name, avg(Volume) AS avg_volume from data group by stock_name)")
    result = spark.sql("select stock_name from temp8_1 where avg_volume = (select max(avg_volume) from temp8_1)")
    return json.loads(result.toPandas().to_json(orient="table", index=False))


@app.route('/highest-and-lowest-price-stock', methods=['POST', 'GET'])
def query9():
    result = spark.sql("SELECT stock_name, max(high) as highest_price, min(low) as lowest_price from data GROUP BY stock_name")
    return json.loads(result.toPandas().to_json(orient="table", index=False))

if __name__ == '__main__':

    spark = SparkSession.builder.master("local").appName("SparkByExamples.com").getOrCreate()

    stock_names = ["ABCB", "ABG", "ABM", "ABTX", "ACA", "ACLS", "ADC", "ADTN", "ADUS", "AEIS", "AEL", "AGO", "AGYS", "AHH", "AIN", "AIR", "AIT", "AJRD", "AKR", "ALEX", "ALG", "ALGT", "ALRM", "AMBC", "AMCX"]
    query1 = {}
    df_stocks = None
    for stock_n in stock_names:

        df3 = spark.read.option("header", "true").csv(f"stock_data/{stock_n}.csv", inferSchema=True)
        df3 = df3.withColumn("stock_name", lit(stock_n))
        df3 = df3.withColumn("Stock_moved", (((col("Close") - col("Open")) / col("Open")) * 100))

        if df_stocks is None:
            df_stocks = df3
        else:
            df_stocks = df_stocks.union(df3)
    df_stocks.createOrReplaceTempView("data")

    app.run(debug=True, port=2001)