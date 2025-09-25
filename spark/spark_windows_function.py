from itertools import groupby

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col, to_date
from custom_log import setup_logger
from datetime import datetime

logging = setup_logger("spark_sql", "spark_sql.log")


def main():
    print("Starting spark sql...")

    spark = SparkSession.builder \
        .appName("SparkSQL") \
        .master("spark://localhost:7077") \
        .getOrCreate()

    logging.info("Connected SparkSQL")

    step1 = datetime.now()

    summarize_df = spark.read.parquet("/home/tungcaobk97vip/spark-data/summary.parquet")

    step2 = datetime.now()
    logging.info(f"Load parquet file in {(step2 - step1).total_seconds()} second")

    # logging.info("Summarize df schema: ")
    # summarize_df.printSchema()
    # summarize_df.show()
    # logging.info(f"Total count: {summarize_df.count()}")
    #
    # country_window = Window.partitionBy("Country").orderBy("WeekNumber")
    # row_num_df = summarize_df.withColumn("RowNum", f.row_number().over(country_window))
    #
    # logging.info("Row num df schema: ")
    # row_num_df.printSchema()
    # row_num_df.show()
    # logging.info(f"Total count: {row_num_df.count()}")

    logging.info("Create spark_view")
    summarize_df.createOrReplaceTempView("spark_view")

    row_num_sql_df = spark.sql(
        """
            select 
                *,
                case
                    when InvoiceValue is null or LAG(InvoiceValue) over (PARTITION BY Country ORDER BY WeekNumber) is null
                    then 0
                    else
                        round(
                            (InvoiceValue - LAG(InvoiceValue) over (PARTITION BY Country ORDER BY WeekNumber)) / 
                            LAG(InvoiceValue) over (PARTITION BY Country ORDER BY WeekNumber) * 100
                            , 2) 
                end AS PercentGrowth,
                round(SUM(InvoiceValue) OVER (
                    PARTITION BY Country ORDER BY WeekNumber
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ), 2) AS AccumulateValue
            from spark_view
            order by Country, WeekNumber
        """
    )
    logging.info("Row num sql df schema: ")
    row_num_sql_df.printSchema()
    row_num_sql_df.show()
    logging.info(f"Total count: {row_num_sql_df.count()}")

    # df_new = join_df.withColumn("date_parsed", to_date(col("FL_DATE"), "dd-MM-yyyy"))

    # logging.info("Create spark_view")
    # summarize_df.createOrReplaceTempView("spark_view")  # temporary use spark_view for query sql

    # cancel_df = spark.sql(
    #     """
    #         select
    #             id, DEST, DEST_CITY_NAME, date_format(TO_DATE(FL_DATE, 'd/M/yyyy'), 'yyyy-MM-dd') as FL_DATE,
    #             ORIGIN, ORIGIN_CITY_NAME, CANCELLED
    #         from spark_view
    #         where YEAR(TO_DATE(FL_DATE, 'd/M/yyyy')) = 2000
    #         and CANCELLED = 1
    #         and DEST_CITY_NAME = 'Atlanta, GA'
    #         order by FL_DATE desc
    #     """
    # )

    # cancel_df = spark.sql(
    #          """
    #             select DEST, YEAR(TO_DATE(FL_DATE, 'd/M/yyyy')) FL_YEAR, count(1) as NUM_CANCELLED_FLIGHT
    #             from spark_view
    #             where CANCELLED = 1 and YEAR(TO_DATE(FL_DATE, 'd/M/yyyy')) is not null
    #             group by DEST, YEAR(TO_DATE(FL_DATE, 'd/M/yyyy'))
    #             order by DEST, YEAR(TO_DATE(FL_DATE, 'd/M/yyyy'))
    #          """
    # )
    # cancel_df.show()
    # logging.info(f"Total cancel count: {cancel_df.count()}")

    logging.info("Stop Spark Join")
    spark.stop()


if __name__ == "__main__":
    main()
