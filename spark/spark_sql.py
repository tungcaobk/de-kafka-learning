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

    # logging.info("Survey df schema: ")
    # survey_df.printSchema()
    # logging.info(survey_df.schema.simpleString())
    # logging.info(survey_df._show_string())
    #

    logging.info("Create spark_view")
    survey_df.createOrReplaceTempView("spark_view")  # temporary use spark_view for query sql

    # gender_df = survey_df.select(col('Gender'), col('Country'),
    #                              f.when(('male' == f.lower(col('Gender'))) | ('m' == f.lower(col('Gender'))), 1)
    #                              .otherwise(0).alias('num_male'))
    # gender_df.show()

    # gender_df = spark.sql(
    #     """
    #         select
    #             Country,
    #             count(
    #                 case
    #                     when lower(Gender) = 'male' or lower(Gender) = 'm' then 1
    #                 end) as num_male
    #         from spark_view
    #         group by Country
    #         order by num_male desc
    #     """
    # )
    # gender_df.show()

    # age40_df = survey_df \
    #     .filter(col('Age') > 40) \
    #     .groupBy('Country') \
    #     .agg(f.count('*').alias('cnt')) \
    #     .orderBy(f.desc('cnt'))
    # age40_df.show()

    count_df = spark.sql(
        """
            select Country, count(1) as cnt from spark_view
            where Age > 40
            group by Country
            order by count(1) desc
        """
    )
    count_df.show()
    # logging.info("Count country by age dataframe:")
    # count_df.printSchema()
    # logging.info(count_df.schema.simpleString())
    # count_df.show()

    logging.info("Stop SparkSQL")
    spark.stop()


if __name__ == "__main__":
    main()
