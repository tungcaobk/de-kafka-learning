from itertools import groupby

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col
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

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path="/home/tungcaobk97vip/spark-data/survey.csv")

    step2 = datetime.now()
    logging.info(f"Load csv file in {(step2 - step1).total_seconds()} second")

    logging.info("Survey df schema: ")
    survey_df.printSchema()
    # survey_df.show()


    logging.info("Create spark_view")
    survey_df.createOrReplaceTempView("spark_view")  # temporary use spark_view for query sql

    udf_df = spark.sql(
        """
                    with castData as (
                        select 
                            Age, Gender, Country, state, no_employees,
                            CAST(regexp_extract(no_employees, '\\\\d+', 0) AS INT) AS cast_employee
                        from spark_view
                    )
                    select 
                        Age, Gender, Country, state, no_employees
                    from castData
                    where cast_employee >= 500
                """
        # """
        #     select
        #         Age, Gender, Country, state, no_employees
        #     from spark_view
        #     where
        #         (
        #           no_employees LIKE 'More than%'
        #           AND CAST(regexp_extract(no_employees, '\\d+', 0) AS INT) >= 500
        #         )
        #         OR
        #         (
        #           regexp_like(no_employees, '^\\d+-\\d+$')
        #           AND CAST(regexp_extract(no_employees, '\\d+', 0) AS INT) >= 500
        #         )
        # """
    )
    logging.info("UDF df schema: ")
    udf_df.printSchema()
    udf_df.show()
    logging.info(f"Total count: {udf_df.count()}")

    logging.info("Stop SparkSQL")
    spark.stop()


if __name__ == "__main__":
    main()
