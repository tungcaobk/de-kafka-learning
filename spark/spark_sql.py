from pyspark.sql import SparkSession
from custom_log import setup_logger

logging = setup_logger("spark_sql", "spark_sql.log")

def main():
    print("Starting spark sql...")

    spark = SparkSession.builder \
        .appName("SparkSQL") \
        .master("spark://localhost:7077") \
        .getOrCreate()

    logging.info("Connected SparkSQL")

    surveyDF = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path = "./survey.csv")

    logging.info("Survey df schema: ")
    surveyDF.printSchema()

    logging.info("Stop SparkSQL")
    spark.stop()

if __name__ == "__main__":
    main()