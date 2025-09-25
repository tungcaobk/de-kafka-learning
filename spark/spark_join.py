from itertools import groupby

from pyspark.sql import SparkSession
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

    flight_1_df = spark.read \
        .json(path="/home/tungcaobk97vip/spark-data/d1/")

    flight_2_df = spark.read \
        .json(path="/home/tungcaobk97vip/spark-data/d2/")

    step2 = datetime.now()
    logging.info(f"Load json file in {(step2 - step1).total_seconds()} second")

    logging.info("Flight 1 df schema: ")
    # flight_1_df.printSchema()
    # flight_1_df.show()

    logging.info("Flight 2 df schema: ")
    # flight_2_df.printSchema()
    # flight_2_df.show()

    join_df = flight_1_df.join(flight_2_df, "id", "inner")
    logging.info("Join schema:")
    join_df.printSchema()
    join_df.show()

    logging.info(f"Total join count: {join_df.count()}")

    # df_new = join_df.withColumn("date_parsed", to_date(col("FL_DATE"), "dd-MM-yyyy"))

    logging.info("Create spark_view")
    join_df.createOrReplaceTempView("spark_view")  # temporary use spark_view for query sql

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

    cancel_df = spark.sql(
             """
                select DEST, YEAR(TO_DATE(FL_DATE, 'd/M/yyyy')) FL_YEAR, count(1) as NUM_CANCELLED_FLIGHT
                from spark_view
                where CANCELLED = 1 and YEAR(TO_DATE(FL_DATE, 'd/M/yyyy')) is not null
                group by DEST, YEAR(TO_DATE(FL_DATE, 'd/M/yyyy'))
                order by DEST, YEAR(TO_DATE(FL_DATE, 'd/M/yyyy'))
             """
    )
    cancel_df.show()
    logging.info(f"Total cancel count: {cancel_df.count()}")

    logging.info("Stop Spark Join")
    spark.stop()


if __name__ == "__main__":
    main()
