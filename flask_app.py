import os

from flask import Flask, render_template
from pyspark.sql.functions import sum, desc, col, avg

from utils import create_spark_session

app = Flask(__name__)
app = Flask(__name__, template_folder='templates')  # still relative to module


@app.route("/")
def index():
    return render_template('index.html')


@app.route("/popular_product")
def get_popular_product():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        row = df.groupBy('Description').agg(
            sum('Quantity').alias('sum_qty')
        ).sort(desc("sum_qty")).collect()[0]
        return {'popular_product': row[0], 'qty_solde': row[1]}
    finally:
        spark.stop()


@app.route("/best_customer")
def get_best_product():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        row = df.filter(df.CustomerID != 'NaN').groupBy('CustomerID').agg(
            sum(col('Quantity') * col('UnitPrice')).alias('TransactionAmount')
        ).sort(desc('TransactionAmount')).collect()[0]
        return {'best_customer_id': row[0], 'total_amount_spent': row[1]}
    finally:
        spark.stop()


@app.route("/average_unit_price")
def get_average_unit_price():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        row = df.agg(avg('UnitPrice')).collect()[0]
        return {'average_unit_price': row[0]}
    finally:
        spark.stop()


@app.route("/price_to_qty_ratio")
def get_price_to_qty_ratio():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        result = df.filter(df.InvoiceNo != 0).groupBy('InvoiceNo').agg(
            sum(col('Quantity') * col('UnitPrice')) / sum(
                col('Quantity'))).collect()
        ret = {}
        for line in result:
            print(line[0], line[1])
            ret[line[0]] = line[1]
        return ret
    finally:
        spark.stop()


# @app.route("/product_distribution_by_country")
# def get_product_distribution_by_country():
#     spark = create_spark_session(__name__, 'mydata', 'online_retail')
#     try:
#         df = spark.sql('select * from online_retail')
#         result = df.groupBy('Country').agg(
#             sum(col('Quantity') * col('UnitPrice')) / sum(
#                 col('Quantity'))).collect()
#         ret = {}
#         for line in result:
#             ret[line[0]] = line[1]
#         return ret
#     finally:
#         spark.stop()


@app.route("/transaction_by_country")
def get_transaction_by_country():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        result = df.groupBy('Country').agg(
            sum(col('Quantity') * col('UnitPrice')).alias(
                'Transaction')).collect()
        ret = {}
        for line in result:
            ret[line[0]] = line[1]
        return ret
    finally:
        spark.stop()


if __name__ == "__main__":
    # os.system('jupyter notebook --no-browser')
    app.run(debug=True, )
