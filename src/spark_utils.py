from pyspark.sql import SparkSession
import logging

def get_spark_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    
    #Reduz a poluição visual no terminal, mostrando apenas avisos e erros críticos
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(name)