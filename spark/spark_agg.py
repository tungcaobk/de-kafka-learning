from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from custom_log import setup_logger
from datetime import datetime

logging = setup_logger("spark_agg", "spark_agg.log")


def main():
    logging.info("Starting spark agg...")

    spark = SparkSession.builder \
        .appName("SparkSQL") \
        .master("spark://localhost:7077") \
        .getOrCreate()

    logging.info("Connected SparkAgg")

    step1 = datetime.now()

    invoice_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/home/tungcaobk97vip/spark-data/invoices.csv")

    step2 = datetime.now()
    logging.info(f"Load csv file in {(step2 - step1).total_seconds()} second")

    # logging.info("Invoice df schema: ")
    # invoice_df.printSchema()
    #
    # logging.info("Show invoice df: ")
    # invoice_df.show()

    logging.info("Create spark_view")
    invoice_df.createOrReplaceTempView("spark_view")  # temporary use spark_view for query sql

    # gender_df = survey_df.select(col('Gender'), col('Country'),
    #                              f.when(('male' == f.lower(col('Gender'))) | ('m' == f.lower(col('Gender'))), 1)
    #                              .otherwise(0).alias('num_male'))
    # gender_df.show()

    num_records = f.count('*').alias('num_records')
    total_quantity = f.sum('Quantity').alias('total_quantity')
    avg_price = f.avg('UnitPrice').alias('avg_price')
    num_invoices = f.count_distinct('InvoiceNo').alias('num_invoices')
    invoice_value = f.round(f.sum(col('Quantity') * col('UnitPrice')), 2).alias('invoice_value')

    # agg_df = invoice_df.select(num_records, total_quantity, avg_price, num_invoices, invoice_value)
    # agg_df.printSchema()
    # agg_df.show()

    # group_df = invoice_df.groupBy('Country') \
    #         .agg(num_invoices, total_quantity, invoice_value) \
    #         .orderBy(f.desc('invoice_value'), col('Country'))

    # test_df = spark.sql(
    #     """
    #         select substr(InvoiceDate, 7, 4) from spark_view
    #     """
    # )
    # test_df.show()

    group_df = spark.sql(
        """
            select 
                Country,
                substr(InvoiceDate, 7, 4) as year,
                count(distinct InvoiceNo) as num_invoices,
                sum(Quantity) as total_quantity,
                round(sum(Quantity * UnitPrice), 2) as invoice_value
            from spark_view
            group by Country, substr(InvoiceDate, 7, 4)
            order by invoice_value desc, Country
        """
    )
    # group_df.show()


    customer_df = spark.sql(
        """
            select 
                CustomerID,
                round(sum(Quantity * UnitPrice), 2) as invoice_value
            from spark_view
            where substr(InvoiceDate, 7, 4) = 2010
            group by CustomerID
            order by invoice_value desc, CustomerID
        """
    )
    customer_df.show()

    logging.info("Stop SparkAgg")
    spark.stop()


if __name__ == "__main__":
    main()
