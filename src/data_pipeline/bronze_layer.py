import os
from src.utils.logger import get_logger
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_date, lit
from dotenv import load_dotenv

# logging setup
logger = get_logger(__name__, log_file="logs/etl_pipeline/bronze.log")

# load .env variables
load_dotenv()

def create_spark_session():
    mongo_connector_pkg = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2" # adjust the connector version to match the PySpark version
    return SparkSession.builder.appName("BronzeLayer_MongoDB_Ingestion").config("spark.jars.packages", mongo_connector_pkg).getOrCreate()

def ingest_bronze_layer():
    spark = create_spark_session()
    
    # config URI read from MongoDB
    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB")
    coll_name = os.getenv("MONGO_COLL_RAW")
    full_mongo_uri = f"{mongo_uri}/{db_name}.{coll_name}"
    
    logger.info(f"Conneting MongoDB: {db_name}.{coll_name}")
    
    try:
        df_raw = spark.read.format("mongo").option("uri", full_mongo_uri).option("sampleSize", 50000).load()
        logger.info(f"Read {df_raw.count()} successfully from MongoDB")
        
        # add metadata
        df_bronze = df_raw.withColumn("_ingested_at", current_timestamp()).withColumn("_ingested_date", to_date(current_timestamp())).withColumn("_source", lit("mongo_scrapy"))
        
        output_dir = os.getenv("DATALAKE_BRONZE_DIR")
        logger.info(f"Writing data in Bronze layer to {output_dir}")
        
        df_bronze.write.format("parquet").mode("append").partitionBy("_ingested_date").save(output_dir)
        logger.info("Successfully pipeline Bronze layer")
    except Exception as e:
        logger.error(f"Error in ETL Bronze: {e}")
        raise
    finally:
        spark.stop()