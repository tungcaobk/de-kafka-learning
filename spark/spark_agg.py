from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from custom_log import setup_logger
from datetime import datetime

logging = setup_logger("spark_sql", "spark_sql.log")


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

    logging.info("Invoice df schema: ")
    invoice_df.printSchema()

    logging.info("Show invoice df: ")
    invoice_df.show()

    logging.info("Create spark_view")
    invoice_df.createOrReplaceTempView("spark_view")  # temporary use spark_view for query sql

    # gender_df = survey_df.select(col('Gender'), col('Country'),
    #                              f.when(('male' == f.lower(col('Gender'))) | ('m' == f.lower(col('Gender'))), 1)
    #                              .otherwise(0).alias('num_male'))
    # gender_df.show()




    logging.info("Stop SparkAgg")
    spark.stop()


if __name__ == "__main__":
    main()
