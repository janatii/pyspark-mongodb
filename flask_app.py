import base64
from io import BytesIO

import matplotlib.pyplot as plt
from flask import Flask, render_template
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as _sum, desc, col, avg

from utils import create_spark_session

app = Flask(__name__, template_folder='templates')  # still relative to module


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/group_by_invoice')
def get_grouped_by_invoice():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        result = df.groupBy('InvoiceNo').agg(F.collect_set('CustomerID'),
                                             F.collect_set('Description'),
                                             F.collect_set('InvoiceDate'),
                                             F.collect_set('Quantity'),
                                             F.collect_set('StockCode'),
                                             F.collect_set('UnitPrice'),
                                             ).collect()
        ret = {}
        for row in result:
            ret[row[0]] = row[1:]
        return ret
    finally:
        spark.stop()


@app.route("/popular_product")
def get_popular_product():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        row = df.groupBy('Description').agg(
            _sum('Quantity').alias('sum_qty')
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
            _sum(col('Quantity') * col('UnitPrice')).alias('TransactionAmount')
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
            _sum(col('Quantity') * col('UnitPrice')) / _sum(
                col('Quantity'))).collect()
        ret = {}
        for line in result:
            print(line[0], line[1])
            ret[line[0]] = line[1]
        return ret
    finally:
        spark.stop()


@app.route("/transaction_by_country")
def get_transaction_by_country():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        result = df.groupBy('Country').agg(
            _sum(col('Quantity') * col('UnitPrice')).alias(
                'Transaction')).collect()
        ret = {}
        for line in result:
            ret[line[0]] = line[1]
        return ret
    finally:
        spark.stop()


@app.route('/products_by_country_distribution')
def chart():
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    df = spark.sql('select * from online_retail')
    row = df.select(F.collect_list('Country')).first()
    countries = set(row[list(row.asDict().keys())[0]])
    return render_template('countries.html',
                           countries=countries)


@app.route('/products_by_country_distribution/<country>')
def get_charts(country):
    spark = create_spark_session(__name__, 'mydata', 'online_retail')
    try:
        df = spark.sql('select * from online_retail')
        products = df.filter(df.Country == country).groupBy(
            'Description').agg(_sum('Quantity').alias('sum'))
        products = products.toPandas()
        labels = products['Description']
        sizes = products['sum']
        sizes[sizes < 0] = 0
        fig1, ax1 = plt.subplots()
        fig1.subplots_adjust(0.3, 0, 1, 1)
        _, _ = ax1.pie(sizes, startangle=90)
        ax1.axis('equal')
        total = sum(list(sizes))
        plt.legend(
            loc='upper left',
            labels=['%s, %1.1f%%' % (
                l, (float(s) / total) * 100) for l, s in zip(labels, sizes)],
            prop={'size': 11},
            bbox_to_anchor=(0.0, 1),
            bbox_transform=fig1.transFigure
        )
        plt.show()
        buf = BytesIO()
        fig1.savefig(buf, format="png")
        # Embed the result in the html output.
        data = base64.b64encode(buf.getbuffer()).decode("ascii")
        return f"<img src='data:image/png;base64,{data}'/>"
    finally:
        spark.stop()


if __name__ == "__main__":
    # os.system('jupyter notebook --no-browser')
    app.run(debug=True)
